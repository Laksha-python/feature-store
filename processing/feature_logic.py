from datetime import timedelta, datetime
from collections import defaultdict


def parse_time(ts):
    if isinstance(ts, datetime):
        return ts
    try:
        return datetime.fromisoformat(str(ts))
    except:
        return None


def compute_user_features(events, reference_time):

    purchase_7d_start = reference_time - timedelta(days=7)
    spend_30d_start = reference_time - timedelta(days=30)

    rolling_7d_purchase_count = defaultdict(int)
    rolling_30d_spend = defaultdict(float)
    latest_event_time = {}

    for e in events:

        t = parse_time(e["event_time"])
        if not t:
            continue

        user = e["user_id"]
        etype = e["event_type"]
        price = float(e.get("price", 0))

        if etype == "purchase" and t >= purchase_7d_start:
            rolling_7d_purchase_count[user] += 1

        if etype == "purchase" and t >= spend_30d_start:
            rolling_30d_spend[user] += price

        if (
            user not in latest_event_time
            or t > latest_event_time[user]
        ):
            latest_event_time[user] = t

    recency_days = {}
    for user, t in latest_event_time.items():
        recency_days[user] = (reference_time - t).days

    return {
        "rolling_7d_purchase_count": dict(rolling_7d_purchase_count),
        "rolling_30d_spend": dict(rolling_30d_spend),
        "recency_days": recency_days
    }

def compute_product_features(events, reference_time):

    start_1h = reference_time - timedelta(hours=1)
    start_24h = reference_time - timedelta(hours=24)

    sales_1h = defaultdict(int)
    sales_24h = defaultdict(int)
    views_24h = defaultdict(int)
    purchases_24h = defaultdict(int)
    refunds_24h = defaultdict(int)

    for e in events:

        t = parse_time(e["event_time"])
        if not t:
            continue

        pid = e["product_id"]
        etype = e["event_type"]

        if t >= start_1h and etype == "purchase":
            sales_1h[pid] += 1

        if t >= start_24h:

            if etype == "purchase":
                sales_24h[pid] += 1
                purchases_24h[pid] += 1

            if etype == "view":
                views_24h[pid] += 1

            if etype == "refund":
                refunds_24h[pid] += 1
    conversion_rate = {}
    for pid in purchases_24h:
        v = views_24h.get(pid, 0)
        p = purchases_24h.get(pid, 0)
        conversion_rate[pid] = p / v if v > 0 else 0

    refund_rate = {}
    for pid in purchases_24h:
        p = purchases_24h.get(pid, 0)
        r = refunds_24h.get(pid, 0)
        refund_rate[pid] = r / p if p > 0 else 0

    return {
        "rolling_1h_sales": dict(sales_1h),
        "rolling_24h_sales": dict(sales_24h),
        "conversion_rate": conversion_rate,
        "refund_rate": refund_rate
    }

def compute_net_revenue_features(events, reference_time):

    start_24h = reference_time - timedelta(hours=24)
    net_revenue = defaultdict(float)

    for e in events:

        t = parse_time(e["event_time"])
        if not t or t < start_24h:
            continue

        user = e["user_id"]
        etype = e["event_type"]
        price = float(e.get("price", 0))

        if etype == "purchase":
            net_revenue[user] += price

        elif etype in ("refund", "cancel"):
            net_revenue[user] -= price

    return dict(net_revenue)
