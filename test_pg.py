import psycopg2

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5433,
    database="feature_store",
    user="user",
    password="password"
)

print("CONNECTED")

conn.close()