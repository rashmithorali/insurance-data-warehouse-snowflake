USE ROLE ACCOUNTADMIN;
USE WAREHOUSE WH_INSURANCE_ANALYTICS_XS;
USE SCHEMA INSURANCE_ANALYTICS_DB.STAGING;

CREATE OR REPLACE PROCEDURE SP_PROCESS_INSURANCE_CDC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run_pipeline'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, to_date, to_timestamp, current_timestamp

def run_pipeline(session: Session) -> str:
    processed_msgs = []

    # --- 1. PROCESS POLICIES STREAM (SCD TYPE 2 FOR ENDORSEMENTS) ---
    has_pol_data = session.sql(
        "SELECT SYSTEM$STREAM_HAS_DATA('INSURANCE_ANALYTICS_DB.RAW.RAW_POLICIES_STREAM')"
    ).collect()[0][0]

    if has_pol_data:
        try:
            session.sql("BEGIN TRANSACTION").collect()

            raw_pol_df = session.table("INSURANCE_ANALYTICS_DB.RAW.RAW_POLICIES_STREAM")

            parsed_pol = raw_pol_df.select(
                col("RAW_PAYLOAD")["policy_id"].astype("STRING").alias("POLICY_ID"),
                col("RAW_PAYLOAD")["broker_id"].astype("STRING").alias("BROKER_ID"),
                col("RAW_PAYLOAD")["customer_id"].astype("STRING").alias("CUSTOMER_ID"),
                col("RAW_PAYLOAD")["product_line"].astype("STRING").alias("PRODUCT_LINE"),
                col("RAW_PAYLOAD")["policy_status"].astype("STRING").alias("POLICY_STATUS"),
                to_date(col("RAW_PAYLOAD")["effective_date"].astype("STRING")).alias("EFFECTIVE_DATE"),
                to_date(col("RAW_PAYLOAD")["expiry_date"].astype("STRING")).alias("EXPIRY_DATE"),
                col("RAW_PAYLOAD")["gross_written_premium"].astype("NUMBER(12,2)").alias("GROSS_WRITTEN_PREMIUM"),
                col("RAW_PAYLOAD")["coverages"]["deductible"].astype("NUMBER(10,2)").alias("DEDUCTIBLE"),
                col("RAW_PAYLOAD")["coverages"]["coverage_limit"].astype("NUMBER(12,2)").alias("COVERAGE_LIMIT"),
                col("RAW_PAYLOAD")["currency"].astype("STRING").alias("CURRENCY"),
                to_timestamp(col("RAW_PAYLOAD")["event_timestamp"].astype("STRING")).alias("EVENT_TS")
            )

            # Writing from the stream into a permanent/transient table consumes the stream
            # within this transaction; if anything below fails, ROLLBACK resets the stream.
            parsed_pol.write.mode("overwrite").save_as_table("TEMP_POLICIES_BATCH")

            # Close out previous active policy versions
            session.sql("""
                UPDATE INSURANCE_ANALYTICS_DB.STAGING.STG_POLICIES_SCD2 target
                SET VALID_TO = batch.EVENT_TS,
                    IS_CURRENT = FALSE
                FROM INSURANCE_ANALYTICS_DB.STAGING.TEMP_POLICIES_BATCH batch
                WHERE target.POLICY_ID = batch.POLICY_ID
                  AND target.IS_CURRENT = TRUE
                  AND batch.EVENT_TS >= target.VALID_FROM
            """).collect()

            # Insert new active policy versions
            session.sql("""
                INSERT INTO INSURANCE_ANALYTICS_DB.STAGING.STG_POLICIES_SCD2 (
                    POLICY_ID, BROKER_ID, CUSTOMER_ID, PRODUCT_LINE, POLICY_STATUS,
                    EFFECTIVE_DATE, EXPIRY_DATE, GROSS_WRITTEN_PREMIUM, DEDUCTIBLE,
                    COVERAGE_LIMIT, CURRENCY, VALID_FROM, VALID_TO, IS_CURRENT
                )
                WITH deduplicated_batch AS (
                    SELECT *
                    FROM INSURANCE_ANALYTICS_DB.STAGING.TEMP_POLICIES_BATCH
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY POLICY_ID, EVENT_TS 
                        ORDER BY EVENT_TS
                    ) = 1
                )
                SELECT 
                    POLICY_ID, BROKER_ID, CUSTOMER_ID, PRODUCT_LINE, POLICY_STATUS,
                    EFFECTIVE_DATE, EXPIRY_DATE, GROSS_WRITTEN_PREMIUM, DEDUCTIBLE,
                    COVERAGE_LIMIT, CURRENCY, EVENT_TS,
                    COALESCE(
                        LEAD(EVENT_TS) OVER (PARTITION BY POLICY_ID ORDER BY EVENT_TS),
                        TO_TIMESTAMP_NTZ('9999-12-31 23:59:59')
                    ) AS VALID_TO,
                    CASE 
                        WHEN LEAD(EVENT_TS) OVER (PARTITION BY POLICY_ID ORDER BY EVENT_TS) IS NULL THEN TRUE
                        ELSE FALSE
                    END AS IS_CURRENT
                FROM deduplicated_batch
            """).collect()

            session.sql("COMMIT").collect()
            processed_msgs.append("Policies SCD2 updated.")

        except Exception as e:
            session.sql("ROLLBACK").collect()
            raise RuntimeError(f"Policies SCD2 failed and rolled back: {e}")
    else:
        processed_msgs.append("No new policy records in stream.")

    # --- 2. PROCESS CLAIMS STREAM ---
    has_clm_data = session.sql(
        "SELECT SYSTEM$STREAM_HAS_DATA('INSURANCE_ANALYTICS_DB.RAW.RAW_CLAIMS_STREAM')"
    ).collect()[0][0]

    if has_clm_data:
        try:
            session.sql("BEGIN TRANSACTION").collect()

            raw_clm_df = session.table("INSURANCE_ANALYTICS_DB.RAW.RAW_CLAIMS_STREAM")

            parsed_clm = raw_clm_df.select(
                col("RAW_PAYLOAD")["claim_id"].astype("STRING").alias("CLAIM_ID"),
                col("RAW_PAYLOAD")["policy_id"].astype("STRING").alias("POLICY_ID"),
                col("RAW_PAYLOAD")["broker_id"].astype("STRING").alias("BROKER_ID"),
                to_date(col("RAW_PAYLOAD")["loss_date"].astype("STRING")).alias("LOSS_DATE"),
                to_date(col("RAW_PAYLOAD")["reported_date"].astype("STRING")).alias("REPORTED_DATE"),
                col("RAW_PAYLOAD")["claim_status"].astype("STRING").alias("CLAIM_STATUS"),
                col("RAW_PAYLOAD")["reserve_amount"].astype("NUMBER(12,2)").alias("RESERVE_AMOUNT"),
                col("RAW_PAYLOAD")["paid_amount"].astype("NUMBER(12,2)").alias("PAID_AMOUNT"),
                (col("RAW_PAYLOAD")["reserve_amount"].astype("NUMBER(12,2)") + col("RAW_PAYLOAD")["paid_amount"].astype("NUMBER(12,2)")).alias("TOTAL_INCURRED"),
                col("RAW_PAYLOAD")["currency"].astype("STRING").alias("CURRENCY"),
                current_timestamp().alias("UPDATED_AT")
            )

            parsed_clm.write.mode("overwrite").save_as_table("TEMP_CLAIMS_BATCH")

            session.sql("""
                MERGE INTO INSURANCE_ANALYTICS_DB.STAGING.STG_CLAIMS target
                USING INSURANCE_ANALYTICS_DB.STAGING.TEMP_CLAIMS_BATCH batch
                ON target.CLAIM_ID = batch.CLAIM_ID
                WHEN MATCHED THEN UPDATE SET
                    CLAIM_STATUS = batch.CLAIM_STATUS,
                    RESERVE_AMOUNT = batch.RESERVE_AMOUNT,
                    PAID_AMOUNT = batch.PAID_AMOUNT,
                    TOTAL_INCURRED = batch.TOTAL_INCURRED,
                    UPDATED_AT = batch.UPDATED_AT
                WHEN NOT MATCHED THEN INSERT (
                    CLAIM_ID, POLICY_ID, BROKER_ID, LOSS_DATE, REPORTED_DATE,
                    CLAIM_STATUS, RESERVE_AMOUNT, PAID_AMOUNT, TOTAL_INCURRED, CURRENCY, UPDATED_AT
                ) VALUES (
                    batch.CLAIM_ID, batch.POLICY_ID, batch.BROKER_ID, batch.LOSS_DATE, batch.REPORTED_DATE,
                    batch.CLAIM_STATUS, batch.RESERVE_AMOUNT, batch.PAID_AMOUNT, batch.TOTAL_INCURRED, batch.CURRENCY, batch.UPDATED_AT
                )
            """).collect()

            session.sql("COMMIT").collect()
            processed_msgs.append("Claims merged.")

        except Exception as e:
            session.sql("ROLLBACK").collect()
            raise RuntimeError(f"Claims merge failed and rolled back: {e}")
    else:
        processed_msgs.append("No new claim records in stream.")

    return " | ".join(processed_msgs)
$$;