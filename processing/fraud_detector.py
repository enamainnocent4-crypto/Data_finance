def detect_fraud(tx):
    if tx is None:
        return False

    try:
        amount = float(tx.get("amount", 0))
    except:
        return True

    return amount > 7000