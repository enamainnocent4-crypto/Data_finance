import json
import random
import uuid
import time
import socket
import os

from datetime import datetime
from kafka import KafkaProducer

print(f"FILE = {__file__}")

countries = ["FR", "DE", "US", "CM", "NG"]
# KAFKA CONNECTION
producer = KafkaProducer(
    bootstrap_servers="localhost:29092",  
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

producer_id = str(uuid.uuid4())

print(
    f"Producer started | "
    f"ID={producer_id} | "
    f"HOST={socket.gethostname()} | "
    f"PID={os.getpid()}"
)
# GENERATE TRANSACTION
def generate_transaction():

    tx = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": str(random.randint(1, 10)),
        "amount": float(random.randint(10, 12000)),
        "country": random.choice(countries),
        "timestamp": datetime.utcnow().isoformat(),
        "producer_id": producer_id
    }

    return tx

# STREAM LOOP
while True:

    tx = generate_transaction()

    print("")
    print("SENDING TX")
    print(tx)

    producer.send("transactions", tx)
    producer.flush()

    print("SENT")

    time.sleep(2)