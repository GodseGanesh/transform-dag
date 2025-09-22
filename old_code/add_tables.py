import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    host="93.127.206.37",
    port="5432",
    dbname="bond_market_temp",
    user="postgres",
    password="postgres"
)

cur = conn.cursor()

# Load SQL file
with open("create_tables.sql", "r") as f:
    sql = f.read()
    cur.execute(sql)

conn.commit()
cur.close()
conn.close()

print("SQL executed successfully!")
