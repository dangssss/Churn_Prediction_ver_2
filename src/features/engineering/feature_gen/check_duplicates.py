"""Quick check: Does cas_customer have duplicate (cms_code_enc, report_month)?

Usage:
    python -m features.engineering.feature_gen.check_duplicates

If output shows max_count = 1, then each (customer, month) has exactly 1 row
and the optimization is guaranteed to produce identical output.
"""
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from config.db_config import PostgresConfig

load_dotenv()

database_url = os.environ.get("DATABASE_URL")
if not database_url:
    cfg = PostgresConfig.from_env()
    database_url = f"postgresql+psycopg2://{cfg.user}:{cfg.password}@{cfg.host}:{cfg.port}/{cfg.dbname}"

engine = create_engine(database_url)

sql = text("""
    SELECT
        MAX(cnt) AS max_rows_per_customer_month,
        COUNT(*) AS total_groups,
        COUNT(*) FILTER (WHERE cnt > 1) AS groups_with_duplicates,
        SUM(cnt) FILTER (WHERE cnt > 1) AS total_duplicate_rows
    FROM (
        SELECT cms_code_enc, report_month, COUNT(*) AS cnt
        FROM public.cas_customer
        GROUP BY cms_code_enc, report_month
    ) grouped
""")

with engine.connect() as conn:
    result = conn.execute(sql).fetchone()

print("=" * 60)
print("cas_customer DUPLICATE CHECK")
print("=" * 60)
print(f"  Max rows per (customer, month) : {result[0]}")
print(f"  Total (customer, month) groups : {result[1]}")
print(f"  Groups with duplicates (>1 row): {result[2]}")
print(f"  Total duplicate rows           : {result[3]}")
print("=" * 60)

if result[0] == 1:
    print("✅ RESULT: Exactly 1 row per (customer, month)")
    print("   → Optimization output is 100% identical to original")
else:
    print(f"⚠️  RESULT: Up to {result[0]} rows per (customer, month)")
    print(f"   → {result[2]} groups have duplicates")
    print("   → Need to adjust sliding_aggregate.sql for AVG/STDDEV/slopes")
