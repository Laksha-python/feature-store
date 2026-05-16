from datetime import datetime, timedelta
from collections import defaultdict
def parse_time(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
        except:
            try:
                return datetime.fromisoformat(ts.replace("Z", ""))
            except:
                return None

def revenue_delta(event_type, price):
    try:
        price = float(price)
    except:
        return 0.0

    if event_type == "purchase":
        return price
    elif event_type in ["refund", "cancel"]:
        return -price
    return 0.0

def compute_user_features(events, reference_time):
    purchase_7d = defaultdict(int)
    spend_30d = defaultdict(float)
    last_event_time = {}
    all_users = set()
    window_7d = reference_time - timedelta(days=7)
    window_30d = reference_time - timedelta(days=30)

    for event in events:
        event_time = parse_time(event.get("event_time"))
        if event_time is None:
            continue
        user = event.get("user_id")
        event_type = event.get("event_type")
        price = event.get("price", 0)

        if not user:
            continue

        all_users.add(user)

        if event_type == "purchase" and event_time >= window_7d:
            purchase_7d[user] += 1

        if event_type == "purchase" and event_time >= window_30d:
            try:
                spend_30d[user] += float(price)
            except:
                pass

        if user not in last_event_time or event_time > last_event_time[user]:
            last_event_time[user] = event_time

    recency_days = {}

    for user in all_users:
        if user in last_event_time:
            recency_days[user] = (reference_time - last_event_time[user]).days
        else:
            recency_days[user] = 999

    return {
        "rolling_7d_purchase_count": {u: purchase_7d.get(u, 0) for u in all_users},
        "rolling_30d_spend": {u: spend_30d.get(u, 0.0) for u in all_users},
        "recency_days": recency_days
    }

def compute_product_features(events, reference_time):
    sales_1h = defaultdict(int)
    sales_24h = defaultdict(int)
    purchases = defaultdict(int)
    views = defaultdict(int)
    refunds = defaultdict(int)
    all_products = set()
    window_1h = reference_time - timedelta(hours=1)
    window_24h = reference_time - timedelta(hours=24)
    for event in events:
        event_time = parse_time(event.get("event_time"))
        if event_time is None:
            continue
        product = event.get("product_id")
        event_type = event.get("event_type")
        if not product:
            continue
        all_products.add(product)
        if event_type == "purchase":
            purchases[product] += 1
            if event_time >= window_1h:
                sales_1h[product] += 1

            if event_time >= window_24h:
                sales_24h[product] += 1

        elif event_type == "view":
            views[product] += 1

        elif event_type in ["refund", "cancel"]:
            refunds[product] += 1

    conversion_rate = {}
    refund_rate = {}

    for product in all_products:

        total_views = views.get(product, 0)
        total_purchases = purchases.get(product, 0)
        total_refunds = refunds.get(product, 0)

        conversion_rate[product] = (
            total_purchases / total_views if total_views > 0 else 0
        )

        refund_rate[product] = (
            total_refunds / total_purchases if total_purchases > 0 else 0
        )

    return {
        "rolling_1h_sales": {p: sales_1h.get(p, 0) for p in all_products},
        "rolling_24h_sales": {p: sales_24h.get(p, 0) for p in all_products},
        "conversion_rate": conversion_rate,
        "refund_rate": refund_rate
    }

def compute_net_revenue_features(events, reference_time):
    revenue = defaultdict(float)
    all_users = set()
    for event in events:
        user = event.get("user_id")
        if not user:
            continue
        all_users.add(user)
        delta = revenue_delta(
            event.get("event_type"),
            event.get("price", 0)
        )
        revenue[user] += delta
    return {u: revenue.get(u, 0.0) for u in all_users}

def compute_error_features(dlq_events, total_events):
    error_count = len(dlq_events)
    error_rate = (
        error_count / total_events
        if total_events > 0 else 0
    )
    return {
        "error_rate_last_10min": error_rate
    }