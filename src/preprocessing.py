"""
preprocessing.py
-----------------
Reads raw Traffic_Violations.csv, cleans every column as documented
in the instructions, engineers new features, and saves the cleaned
dataset to data/processed/cleaned_traffic.csv.

Run:
    python src/preprocessing.py
"""

import os
import re
import pandas as pd
import numpy as np
from tqdm import tqdm

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH    = os.path.join(BASE_DIR, "dataset", "Traffic_Violations.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed", "cleaned_traffic.csv")

# ── Helpers ────────────────────────────────────────────────────────────────────

def standardize_bool(series: pd.Series) -> pd.Series:
    """Convert Yes/No/Y/N/True/False/blank variants to boolean (nullable)."""
    mapping = {
        "yes": True,  "y": True,  "true": True,  "1": True,
        "no": False,  "n": False, "false": False, "0": False,
    }
    return (
        series.astype(str)
              .str.strip()
              .str.lower()
              .map(mapping)
              .astype("boolean")
    )


def time_of_day_bucket(hour: int) -> str:
    """Assign a time-of-day label based on hour (0–23)."""
    if   5  <= hour < 12: return "Morning"
    elif 12 <= hour < 17: return "Afternoon"
    elif 17 <= hour < 21: return "Evening"
    else:                  return "Night"


# ── Make name normalisation map ────────────────────────────────────────────────
MAKE_MAP = {
    "CHEV": "CHEVROLET", "CHEVY": "CHEVROLET",
    "MERZ": "MERCEDES-BENZ", "MERC": "MERCURY",
    "TOYT": "TOYOTA",    "TOYO": "TOYOTA",
    "HOND": "HONDA",
    "FORD": "FORD",
    "NISS": "NISSAN",    "NISA": "NISSAN",
    "HYUN": "HYUNDAI",
    "DODG": "DODGE",
    "JEEP": "JEEP",
    "VOLK": "VOLKSWAGEN", "VW": "VOLKSWAGEN",
    "BENZ": "MERCEDES-BENZ",
    "ACUR": "ACURA",
    "INFI": "INFINITI",
    "LEXU": "LEXUS",
    "MITS": "MITSUBISHI",
    "SUBA": "SUBARU",
    "MAZD": "MAZDA",
    "KIAM": "KIA",       "KIA": "KIA",
    "CADI": "CADILLAC",
    "BUIC": "BUICK",
    "PONT": "PONTIAC",
    "OLDS": "OLDSMOBILE",
    "LINC": "LINCOLN",
    "CHRY": "CHRYSLER",
    "VOLV": "VOLVO",
    "AUDI": "AUDI",
    "PORS": "PORSCHE",
    "LAMB": "LAMBORGHINI",
    "FERR": "FERRARI",
    "HIUM": "HUMMER",    "HUMM": "HUMMER",
    "GMC":  "GMC",
    "SATU": "SATURN",
    "SUZI": "SUZUKI",
    "DAEW": "DAEWOO",
    "SCION":"SCION",
    "MINI": "MINI",
    "FIAT": "FIAT",
    "MASE": "MASERATI",
    "BENT": "BENTLEY",
    "ROLL": "ROLLS-ROYCE",
    "ISUZU":"ISUZU",     "ISUZ": "ISUZU",
    "STER": "STERLING",
    "PANA": "PANAMERA",
}

# Standard color map
COLOR_MAP = {
    "BLK": "BLACK",   "WHI": "WHITE",  "WHT": "WHITE",
    "RED": "RED",     "BLU": "BLUE",   "GRN": "GREEN",
    "GRY": "GRAY",    "GRA": "GRAY",   "SIL": "SILVER",
    "YEL": "YELLOW",  "ORG": "ORANGE", "BRN": "BROWN",
    "PNK": "PINK",    "PRP": "PURPLE", "GLD": "GOLD",
    "TAN": "TAN",     "CPR": "COPPER", "MAR": "MAROON",
    "BGE": "BEIGE",   "CRM": "CREAM",  "NAT": "NATURAL",
    "N/A": np.nan,    "UN":  np.nan,
}


# ── Main cleaning function ─────────────────────────────────────────────────────

def load_and_clean(path: str) -> pd.DataFrame:
    print(f"[1/9] Loading dataset from: {path}")
    df = pd.read_csv(path, low_memory=False)
    print(f"      Loaded {len(df):,} rows × {len(df.columns)} columns")

    # ── 1. Column name normalisation ──────────────────────────────────────────
    print("[2/9] Normalising column names ...")
    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.replace(r"[\s/]+", "_", regex=True)
          .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    # ── 2. Remove full duplicates ─────────────────────────────────────────────
    print("[3/9] Removing duplicate rows ...")
    before = len(df)
    df.drop_duplicates(inplace=True)
    print(f"      Dropped {before - len(df):,} duplicate rows")

    # ── 3. Date & Time ────────────────────────────────────────────────────────
    print("[4/9] Parsing date / time columns ...")
    df["date_of_stop"] = pd.to_datetime(
        df["date_of_stop"], infer_datetime_format=True, errors="coerce"
    )
    # Reject future dates (after today) or clearly wrong years
    df.loc[df["date_of_stop"].dt.year > 2026, "date_of_stop"] = pd.NaT

    df["time_of_stop"] = (
        df["time_of_stop"]
          .astype(str)
          .str.strip()
          .str.replace(r"\.", ":", regex=True)   # fix 23.11.00 → 23:11:00
    )
    df["time_of_stop"] = pd.to_datetime(
        df["time_of_stop"], format="%H:%M:%S", errors="coerce"
    ).dt.time

    # ── 4. Boolean columns ────────────────────────────────────────────────────
    print("[5/9] Standardising boolean columns ...")
    bool_cols = [
        "accident", "belts", "personal_injury", "property_damage",
        "fatal", "commercial_license", "hazmat", "commercial_vehicle",
        "alcohol", "work_zone", "search_conducted", "contributed_to_accident",
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = standardize_bool(df[col])

    # ── 5. Coordinates ────────────────────────────────────────────────────────
    print("[6/9] Cleaning coordinates ...")
    df["latitude"]  = pd.to_numeric(df["latitude"],  errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # 0.0 is invalid for US locations
    df.loc[df["latitude"]  == 0.0, "latitude"]  = np.nan
    df.loc[df["longitude"] == 0.0, "longitude"] = np.nan

    # US bounding box: lat 24–50, lon -125 to -65
    df.loc[~df["latitude"].between(24, 50),   "latitude"]  = np.nan
    df.loc[~df["longitude"].between(-125,-65),"longitude"] = np.nan

    # Extract geolocation tuple if present and drop the raw column
    if "geolocation" in df.columns:
        df.drop(columns=["geolocation"], inplace=True)

    # ── 6. Categorical / text columns ────────────────────────────────────────
    print("[7/9] Standardising categorical columns ...")

    # Agency / SubAgency
    for col in ["agency", "sub_agency", "subagency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    # State columns → uppercase 2-letter
    for col in ["state", "driver_state", "dl_state"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
            df.loc[df[col].str.len() != 2, col] = np.nan

    # Race
    if "race" in df.columns:
        df["race"] = df["race"].astype(str).str.strip().str.upper()
        df.loc[df["race"].isin(["NAN", "", "UNKNOWN", "U"]), "race"] = np.nan

    # Gender → M / F / Unknown
    if "gender" in df.columns:
        df["gender"] = df["gender"].astype(str).str.strip().str.upper()
        df["gender"] = df["gender"].map(
            lambda x: "M" if x == "M" else ("F" if x == "F" else "Unknown")
        )

    # Violation type → title case
    if "violation_type" in df.columns:
        df["violation_type"] = (
            df["violation_type"].astype(str).str.strip().str.title()
        )

    # Vehicle make
    if "make" in df.columns:
        df["make"] = df["make"].astype(str).str.strip().str.upper()
        df["make"] = df["make"].replace(MAKE_MAP)

    # Vehicle model → uppercase
    if "model" in df.columns:
        df["model"] = df["model"].astype(str).str.strip().str.upper()

    # Vehicle color
    if "color" in df.columns:
        df["color"] = df["color"].astype(str).str.strip().str.upper()
        df["color"] = df["color"].replace(COLOR_MAP)

    # Vehicle year
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df.loc[~df["year"].between(1960, 2026), "year"] = np.nan

    # Description / Location → strip whitespace
    for col in ["description", "location", "driver_city"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # Charge / Article → strip
    for col in ["charge", "article"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Search disposition / outcome / type → fill blanks with "Not Applicable"
    for col in ["search_disposition", "search_outcome", "search_type"]:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.strip()
                       .replace({"nan": "Not Applicable", "": "Not Applicable"})
            )

    # ── 7. Feature Engineering ────────────────────────────────────────────────
    print("[8/9] Engineering new features ...")

    # stop_hour & time_of_day
    df["stop_hour"] = df["time_of_stop"].apply(
        lambda t: t.hour if pd.notna(t) and t is not None else np.nan
    )
    df["time_of_day"] = df["stop_hour"].apply(
        lambda h: time_of_day_bucket(int(h)) if pd.notna(h) else "Unknown"
    )

    # day_of_week & month from date_of_stop
    df["day_of_week"] = df["date_of_stop"].dt.day_name()
    df["month"]       = df["date_of_stop"].dt.month
    df["year_of_stop"]= df["date_of_stop"].dt.year

    # ── 8. Optimise dtypes ────────────────────────────────────────────────────
    df["stop_hour"]  = df["stop_hour"].astype("Int8",   errors="ignore")  # type: ignore[arg-type]
    df["month"]      = df["month"].astype("Int8",       errors="ignore")  # type: ignore[arg-type]
    df["year_of_stop"] = df["year_of_stop"].astype("Int16", errors="ignore")  # type: ignore[arg-type]

    for col in ["agency", "sub_agency", "subagency", "state", "driver_state",
                "dl_state", "race", "gender", "violation_type", "color",
                "time_of_day", "day_of_week", "vehicle_type", "vehicletype",
                "make", "arrest_type"]:
        if col in df.columns:
            df[col] = df[col].astype("category")

    print(f"      Final shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    df = load_and_clean(RAW_PATH)

    print(f"[9/9] Saving cleaned dataset to: {OUTPUT_PATH}")
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)
    print("✅  Preprocessing complete!")

    # Quick summary
    print("\n── Column null counts (top 15) ──")
    print(df.isnull().sum().sort_values(ascending=False).head(15).to_string())


if __name__ == "__main__":
    main()
