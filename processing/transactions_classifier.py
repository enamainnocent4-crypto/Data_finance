def is_invalid(tx):
    if tx is None:
        return True

    if not tx.get("transaction_id"):
        return True

    if not tx.get("user_id"):
        return True

    amount = tx.get("amount")
    if amount is None:
        return True

    try:
        float(amount)
    except:
        return True

    return False