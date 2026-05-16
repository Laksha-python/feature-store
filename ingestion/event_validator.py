VALID_EVENT_TYPES = {"purchase", "refund", "cancel"}
def validate_event(event):
    errors = []
    if not event.get("user_id"):
        errors.append("missing_user_id")
    try:
        float(event.get("price", 0))
    except:
        errors.append("invalid_price")
    if event.get("event_type") not in VALID_EVENT_TYPES:
        errors.append("invalid_event_type")
    return len(errors) == 0, errors
