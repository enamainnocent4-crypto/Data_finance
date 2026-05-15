import random
import uuid
from datetime import datetime

countries = ["FR", "DE", "US", "CM", "NG"]

def generate_transaction():
    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1, 10)),
        "amount": float(random.randint(10, 12000)),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat()
    }