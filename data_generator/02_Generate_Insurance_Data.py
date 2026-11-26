import json
import random
from datetime import date, timedelta

# Simulate 3 multi-tenant insurance brokers (crucial for SaaS architecture)
BROKERS = ["BRK_NL_001", "BRK_NL_002", "BRK_NL_003"]
PRODUCTS = ["AUTO_COMP", "HOME_BUILDINGS", "LIABILITY_PRO", "TRAVEL_ANNUAL"]
STATUSES = ["NEW", "RENEWAL", "ENDORSEMENT", "CANCELLED"]

def generate_policies(num_records=50):
    policies = []
    base_date = date(2026, 1, 1)
    
    for i in range(1, num_records + 1):
        policy_id = f"POL-{10000 + i}"
        broker_id = random.choice(BROKERS)
        effective_date = base_date + timedelta(days=random.randint(0, 300))
        expiry_date = effective_date + timedelta(days=365)
        gwp = round(random.uniform(250.0, 4500.0), 2)
        
        policies.append({
            "policy_id": policy_id,
            "broker_id": broker_id,
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product_line": random.choice(PRODUCTS),
            "policy_status": random.choice(["NEW", "RENEWAL"]),
            "effective_date": str(effective_date),
            "expiry_date": str(expiry_date),
            "gross_written_premium": gwp,
            "currency": "EUR",
            "coverages": {
                "deductible": random.choice([0, 150, 300, 500]),
                "coverage_limit": random.choice([50000, 250000, 1000000])
            },
            "event_timestamp": f"{effective_date}T09:15:00Z"
        })

    # Add 5 mid-term endorsements (updates to existing policies for SCD Type 2 demo)
    for j in range(1, 6):
        orig = policies[j]
        endorsement_date = date.fromisoformat(orig["effective_date"]) + timedelta(days=45)
        policies.append({
            **orig,
            "policy_status": "ENDORSEMENT",
            "gross_written_premium": round(orig["gross_written_premium"] + 180.00, 2),
            "event_timestamp": f"{endorsement_date}T14:30:00Z"
        })
        
    return policies

def generate_claims(policies, num_claims=20):
    claims = []
    sampled_policies = random.sample(policies[:50], num_claims)
    
    for idx, pol in enumerate(sampled_policies, 1):
        eff_date = date.fromisoformat(pol["effective_date"])
        loss_date = eff_date + timedelta(days=random.randint(5, 120))
        reported_date = loss_date + timedelta(days=random.randint(0, 14))
        reserve = round(random.uniform(500.0, 12000.0), 2)
        paid = round(reserve * random.uniform(0.0, 1.0), 2)
        
        claims.append({
            "claim_id": f"CLM-{50000 + idx}",
            "policy_id": pol["policy_id"],
            "broker_id": pol["broker_id"],
            "loss_date": str(loss_date),
            "reported_date": str(reported_date),
            "claim_status": random.choice(["OPEN", "SETTLED", "UNDER_REVIEW"]),
            "reserve_amount": reserve,
            "paid_amount": paid,
            "currency": "EUR",
            "event_timestamp": f"{reported_date}T11:00:00Z"
        })
    return claims

if __name__ == "__main__":
    policies_data = generate_policies()
    claims_data = generate_claims(policies_data)
    
    with open("policies_batch_01.json", "w") as f:
        for record in policies_data:
            f.write(json.dumps(record) + "\n")
            
    with open("claims_batch_01.json", "w") as f:
        for record in claims_data:
            f.write(json.dumps(record) + "\n")
            
    print("Generated policies_batch_01.json and claims_batch_01.json successfully.")