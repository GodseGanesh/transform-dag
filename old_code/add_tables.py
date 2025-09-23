import psycopg2

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="bond_market",
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
