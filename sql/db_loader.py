"""
db_loader.py
------------
Loads the cleaned traffic violations CSV into a MySQL database.
Reads credentials from the .env file in the project root.

Run:
    python sql/db_loader.py
"""

import os
import math
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm
from urllib.parse import quote_plus

# ── Load .env ──────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(BASE_DIR, ".env"))

MYSQL_HOST     = os.getenv("MYSQL_HOST",     "localhost")
MYSQL_PORT     = os.getenv("MYSQL_PORT",     "3306")
MYSQL_USER     = os.getenv("MYSQL_USER",     "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "traffic_violations")

# ── Paths ──────────────────────────────────────────────────────────────────────
CLEANED_CSV   = os.path.join(BASE_DIR, "data", "processed", "cleaned_traffic.csv")
SQL_SCHEMA    = os.path.join(BASE_DIR, "sql", "create_tables.sql")

# Chunk size for bulk inserts (tune based on RAM)
CHUNK_SIZE = 10_000


# ── Column mapping: CSV column → DB column ────────────────────────────────────
# Handles both possible column names from preprocessing output
RENAME_MAP = {
    "seqid":                   "seq_id",
    "date_of_stop":            "date_of_stop",
    "time_of_stop":            "time_of_stop",
    "agency":                  "agency",
    "sub_agency":              "sub_agency",
    "subagency":               "sub_agency",
    "description":             "description",
    "location":                "location",
    "latitude":                "latitude",
    "longitude":               "longitude",
    "accident":                "accident",
    "belts":                   "belts",
    "personal_injury":         "personal_injury",
    "property_damage":         "property_damage",
    "fatal":                   "fatal",
    "commercial_license":      "commercial_license",
    "hazmat":                  "hazmat",
    "commercial_vehicle":      "commercial_vehicle",
    "alcohol":                 "alcohol",
    "work_zone":               "work_zone",
    "search_conducted":        "search_conducted",
    "contributed_to_accident": "contributed_to_accident",
    "search_disposition":      "search_disposition",
    "search_outcome":          "search_outcome",
    "search_reason":           "search_reason",
    "search_reason_for_stop":  "search_reason_for_stop",
    "search_type":             "search_type",
    "search_arrest_reason":    "search_arrest_reason",
    "state":                   "state",
    "driver_city":             "driver_city",
    "driver_state":            "driver_state",
    "dl_state":                "dl_state",
    "vehicletype":             "vehicle_type",
    "vehicle_type":            "vehicle_type",
    "year":                    "vehicle_year",
    "make":                    "make",
    "model":                   "model",
    "color":                   "color",
    "violation_type":          "violation_type",
    "charge":                  "charge",
    "article":                 "article",
    "arrest_type":             "arrest_type",
    "race":                    "race",
    "gender":                  "gender",
    "stop_hour":               "stop_hour",
    "time_of_day":             "time_of_day",
    "day_of_week":             "day_of_week",
    "month":                   "month",
    "year_of_stop":            "year_of_stop",
}

# DB columns that are boolean flags
BOOL_COLS = [
    "accident", "belts", "personal_injury", "property_damage",
    "fatal", "commercial_license", "hazmat", "commercial_vehicle",
    "alcohol", "work_zone", "search_conducted", "contributed_to_accident",
]


def _encoded_password() -> str:
    return quote_plus(MYSQL_PASSWORD)


def get_server_engine():
    """Engine connected to MySQL server (no specific DB selected)."""
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{_encoded_password()}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/"
        f"?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)


def get_db_engine():
    """Engine connected to target database."""
    url = (
        f"mysql+pymysql://{MYSQL_USER}:{_encoded_password()}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
        f"?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_database_exists(server_engine) -> None:
    """Create target database if it does not exist."""
    with server_engine.connect() as conn:
        conn.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        )
        conn.commit()


def run_schema(engine) -> None:
    """Execute create_tables.sql to ensure DB / table exist."""
    print("[1/4] Running schema script ...")
    with open(SQL_SCHEMA, "r") as f:
        raw = f.read()

    # Split on semicolons, skip empty / comment-only statements
    statements = [s.strip() for s in raw.split(";") if s.strip() and not s.strip().startswith("--")]
    with engine.connect() as conn:
        for stmt in statements:
            if stmt:
                try:
                    conn.execute(text(stmt))
                    conn.commit()
                except Exception as e:
                    # Ignore "already exists" type warnings
                    if "already exists" not in str(e).lower():
                        print(f"   ⚠️  {e}")
    print("   ✅  Schema ready.")


def load_csv(path: str) -> pd.DataFrame:
    print(f"[2/4] Loading cleaned CSV: {path}")
    df = pd.read_csv(path, low_memory=False)
    print(f"      {len(df):,} rows loaded.")

    # Rename columns to match DB schema
    df.rename(columns={k: v for k, v in RENAME_MAP.items() if k in df.columns}, inplace=True)

    # Keep only columns that exist in DB
    db_cols = list(RENAME_MAP.values())
    df = df[[c for c in df.columns if c in db_cols]]

    # Convert booleans: True/False/true/false/1/0 → int (MySQL TINYINT)
    for col in BOOL_COLS:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                       .str.strip()
                       .str.lower()
                       .map({"true": 1, "false": 0, "1": 1, "0": 0, "nan": None})
            )

    # Fix time_of_stop: keep only HH:MM:SS string for MySQL TIME
    if "time_of_stop" in df.columns:
        df["time_of_stop"] = df["time_of_stop"].astype(str).str.extract(r"(\d{2}:\d{2}:\d{2})")[0]

    # Replace NaN with None for proper NULL insertion
    df = df.where(pd.notna(df), other=None)

    print(f"      {len(df.columns)} columns mapped for DB insert.")
    return df


def insert_chunks(df: pd.DataFrame, engine, table: str = "violations") -> None:
    total_chunks = math.ceil(len(df) / CHUNK_SIZE)
    print(f"[3/4] Inserting {len(df):,} rows in {total_chunks} chunks of {CHUNK_SIZE:,} ...")

    errors = 0
    with tqdm(total=len(df), unit="rows", desc="   Uploading") as pbar:
        for i in range(total_chunks):
            chunk = df.iloc[i * CHUNK_SIZE : (i + 1) * CHUNK_SIZE]
            try:
                chunk.to_sql(
                    name=table,
                    con=engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
            except Exception as e:
                errors += 1
                print(f"\n   ⚠️  Chunk {i+1} error: {e}")
            pbar.update(len(chunk))

    if errors == 0:
        print("   ✅  All rows inserted successfully.")
    else:
        print(f"   ⚠️  Completed with {errors} chunk error(s).")


def verify(engine, table: str = "violations") -> None:
    print("[4/4] Verifying row count in MySQL ...")
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`"))
        count = result.scalar()
    print(f"   ✅  Rows in `{table}` table: {count:,}")


def main():
    print("=" * 55)
    print("  Traffic Violations → MySQL Loader")
    print("=" * 55)
    print(f"  Host     : {MYSQL_HOST}:{MYSQL_PORT}")
    print(f"  Database : {MYSQL_DATABASE}")
    print(f"  User     : {MYSQL_USER}")
    print("=" * 55 + "\n")

    server_engine = get_server_engine()

    # Test server-level connection and create DB if missing
    try:
        with server_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        ensure_database_exists(server_engine)
        print("   🔌  MySQL connection successful.\n")
    except Exception as e:
        print(f"\n❌  Cannot connect to MySQL: {e}")
        print("   → Check your .env credentials and ensure MySQL is running.")
        return

    engine = get_db_engine()

    run_schema(engine)
    df = load_csv(CLEANED_CSV)
    insert_chunks(df, engine)
    verify(engine)

    print("\n🎉  Data load complete!")


if __name__ == "__main__":
    main()
