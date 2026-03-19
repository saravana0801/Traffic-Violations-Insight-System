"""
eda.py
------
Performs structured Exploratory Data Analysis on the cleaned traffic violations
dataset and saves all charts to reports/eda_charts/.

Run:
    python src/eda.py
"""

import os
import warnings
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

warnings.filterwarnings("ignore")

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH  = os.path.join(BASE_DIR, "data", "processed", "cleaned_traffic.csv")
CHARTS_DIR  = os.path.join(BASE_DIR, "reports", "eda_charts")
os.makedirs(CHARTS_DIR, exist_ok=True)

# ── Plot styling ───────────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted")
PALETTE   = "tab10"
FIG_DPI   = 150
TITLE_PAD = 14


def save(fig: plt.Figure, name: str) -> None:
    path = os.path.join(CHARTS_DIR, f"{name}.png")
    fig.savefig(path, dpi=FIG_DPI, bbox_inches="tight")
    plt.close(fig)
    print(f"   💾  Saved → {os.path.relpath(path, BASE_DIR)}")


# ── Load data ──────────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    print(f"[LOAD] Reading cleaned dataset from:\n       {INPUT_PATH}\n")
    df = pd.read_csv(INPUT_PATH, low_memory=False)

    # Parse dates
    df["date_of_stop"] = pd.to_datetime(df["date_of_stop"], errors="coerce")

    # Ensure numeric
    for col in ["stop_hour", "month", "year", "latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"       Shape: {df.shape[0]:,} rows × {df.shape[1]} columns\n")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Q1 — Most common violation types / descriptions
# ══════════════════════════════════════════════════════════════════════════════

def q1_top_violations(df: pd.DataFrame) -> None:
    print("[Q1]  Most common violations ...")

    # --- Violation Type (Citation / Warning / ESERO …)
    if "violation_type" in df.columns:
        vc = df["violation_type"].value_counts().head(10)
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(x=vc.values, y=vc.index, palette=PALETTE, ax=ax)
        ax.set_title("Top 10 Violation Types", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        for p in ax.patches:
            ax.annotate(f"{int(p.get_width()):,}",
                        (p.get_width(), p.get_y() + p.get_height() / 2),
                        xytext=(5, 0), textcoords="offset points", va="center", fontsize=9)
        save(fig, "q1_violation_types")

    # --- Top 15 violation descriptions
    if "description" in df.columns:
        vd = df["description"].value_counts().head(15)
        fig, ax = plt.subplots(figsize=(10, 7))
        sns.barplot(x=vd.values, y=vd.index, palette=PALETTE, ax=ax)
        ax.set_title("Top 15 Violation Descriptions", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q1_violation_descriptions")


# ══════════════════════════════════════════════════════════════════════════════
# Q2 — High-incident locations / sub-agencies
# ══════════════════════════════════════════════════════════════════════════════

def q2_top_locations(df: pd.DataFrame) -> None:
    print("[Q2]  High-incident locations ...")

    # Top sub-agencies
    col = "sub_agency" if "sub_agency" in df.columns else "subagency"
    if col in df.columns:
        sa = df[col].value_counts().head(10)
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(x=sa.values, y=sa.index, palette=PALETTE, ax=ax)
        ax.set_title("Top 10 Sub-Agencies by Incident Count",
                     pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q2_top_subagencies")

    # Top road locations
    if "location" in df.columns:
        loc = df["location"].value_counts().head(15)
        fig, ax = plt.subplots(figsize=(10, 7))
        sns.barplot(x=loc.values, y=loc.index, palette=PALETTE, ax=ax)
        ax.set_title("Top 15 Incident Locations (Road / Intersection)",
                     pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.set_ylabel("")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q2_top_road_locations")


# ══════════════════════════════════════════════════════════════════════════════
# Q3 — Demographics vs violation type
# ══════════════════════════════════════════════════════════════════════════════

def q3_demographics(df: pd.DataFrame) -> None:
    print("[Q3]  Demographics analysis ...")

    # Violations by Race
    if "race" in df.columns:
        rc = df["race"].value_counts().head(10).reset_index()
        rc.columns = ["race", "count"]
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(data=rc, x="count", y="race", palette=PALETTE, ax=ax)
        ax.set_title("Violations by Driver Race", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q3_violations_by_race")

    # Violations by Gender
    if "gender" in df.columns:
        gc = df["gender"].value_counts()
        fig, ax = plt.subplots(figsize=(5, 5))
        ax.pie(gc.values, labels=gc.index, autopct="%1.1f%%",
               colors=sns.color_palette(PALETTE, len(gc)), startangle=140)
        ax.set_title("Violations by Driver Gender", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        save(fig, "q3_violations_by_gender")

    # Race × Violation Type heatmap
    if "race" in df.columns and "violation_type" in df.columns:
        top_races = df["race"].value_counts().head(6).index
        top_vt    = df["violation_type"].value_counts().head(6).index
        sub = df[df["race"].isin(top_races) & df["violation_type"].isin(top_vt)]
        pivot = sub.groupby(["race", "violation_type"]).size().unstack(fill_value=0)
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(pivot, annot=True, fmt=",d", cmap="YlOrRd", ax=ax, linewidths=0.5)
        ax.set_title("Race × Violation Type Heatmap", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_ylabel("Race")
        ax.set_xlabel("Violation Type")
        save(fig, "q3_race_violation_heatmap")


# ══════════════════════════════════════════════════════════════════════════════
# Q4 — Temporal patterns: hour, weekday, month
# ══════════════════════════════════════════════════════════════════════════════

def q4_temporal_patterns(df: pd.DataFrame) -> None:
    print("[Q4]  Temporal patterns ...")

    # By Hour of Day
    if "stop_hour" in df.columns:
        hourly = df["stop_hour"].dropna().astype(int).value_counts().sort_index()
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.bar(hourly.index, hourly.values,
               color=sns.color_palette(PALETTE, 24), edgecolor="white")
        ax.set_title("Violations by Hour of Day", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Hour (0 = Midnight)")
        ax.set_ylabel("Count")
        ax.set_xticks(range(0, 24))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q4_violations_by_hour")

    # By Day of Week
    if "day_of_week" in df.columns:
        day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
        dow = df["day_of_week"].value_counts().reindex(day_order, fill_value=0)
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(x=dow.index, y=dow.values, palette=PALETTE, ax=ax)
        ax.set_title("Violations by Day of Week", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Day")
        ax.set_ylabel("Count")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q4_violations_by_day")

    # By Month
    if "month" in df.columns:
        month_labels = ["Jan","Feb","Mar","Apr","May","Jun",
                        "Jul","Aug","Sep","Oct","Nov","Dec"]
        monthly = df["month"].dropna().astype(int).value_counts().sort_index()
        monthly.index = [month_labels[i-1] for i in monthly.index]
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.lineplot(x=monthly.index, y=monthly.values, marker="o", color="steelblue", ax=ax)
        ax.fill_between(range(len(monthly)), monthly.values, alpha=0.15, color="steelblue")
        ax.set_title("Violations by Month", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Month")
        ax.set_ylabel("Count")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q4_violations_by_month")

    # Time of day buckets
    if "time_of_day" in df.columns:
        tod_order = ["Morning", "Afternoon", "Evening", "Night", "Unknown"]
        tod = df["time_of_day"].value_counts().reindex(tod_order).dropna()
        fig, ax = plt.subplots(figsize=(7, 5))
        sns.barplot(x=tod.index, y=tod.values, palette=PALETTE, ax=ax)
        ax.set_title("Violations by Time-of-Day Bucket",
                     pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Time of Day")
        ax.set_ylabel("Count")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q4_time_of_day_bucket")


# ══════════════════════════════════════════════════════════════════════════════
# Q5 — Vehicle makes / types
# ══════════════════════════════════════════════════════════════════════════════

def q5_vehicle_analysis(df: pd.DataFrame) -> None:
    print("[Q5]  Vehicle analysis ...")

    # Top 15 vehicle makes
    if "make" in df.columns:
        mk = df["make"].value_counts().head(15)
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.barplot(x=mk.values, y=mk.index, palette=PALETTE, ax=ax)
        ax.set_title("Top 15 Vehicle Makes", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q5_top_vehicle_makes")

    # Vehicle type distribution
    vt_col = "vehicletype" if "vehicletype" in df.columns else "vehicle_type"
    if vt_col in df.columns:
        vt = df[vt_col].value_counts().head(10)
        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(x=vt.values, y=vt.index, palette=PALETTE, ax=ax)
        ax.set_title("Vehicle Type Distribution", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q5_vehicle_types")

    # Vehicle manufacture year distribution
    if "year" in df.columns:
        yr = df["year"].dropna()
        fig, ax = plt.subplots(figsize=(12, 5))
        ax.hist(yr, bins=40, color="steelblue", edgecolor="white")
        ax.set_title("Distribution of Vehicle Manufacture Year",
                     pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Year")
        ax.set_ylabel("Count")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q5_vehicle_year_distribution")


# ══════════════════════════════════════════════════════════════════════════════
# Q6 — Accidents, injuries, fatalities
# ══════════════════════════════════════════════════════════════════════════════

def q6_severity_analysis(df: pd.DataFrame) -> None:
    print("[Q6]  Severity analysis ...")

    severity_cols = [
        "accident", "personal_injury", "property_damage",
        "fatal", "alcohol", "belts", "hazmat",
    ]
    present = [c for c in severity_cols if c in df.columns]

    if present:
        counts = {
            col: df[col].astype(str).str.lower().eq("true").sum()
            for col in present
        }
        counts_s = pd.Series(counts).sort_values(ascending=False)

        fig, ax = plt.subplots(figsize=(9, 5))
        sns.barplot(x=counts_s.values, y=counts_s.index, palette="Reds_r", ax=ax)
        ax.set_title("Incident Severity Indicators (True count)",
                     pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Number of Incidents")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        for p in ax.patches:
            ax.annotate(f"{int(p.get_width()):,}",
                        (p.get_width(), p.get_y() + p.get_height() / 2),
                        xytext=(5, 0), textcoords="offset points", va="center", fontsize=9)
        save(fig, "q6_severity_indicators")

    # Contributed to accident breakdown
    if "contributed_to_accident" in df.columns:
        ca = df["contributed_to_accident"].astype(str).str.lower().value_counts()
        fig, ax = plt.subplots(figsize=(5, 5))
        ax.pie(ca.values, labels=ca.index.str.title(), autopct="%1.1f%%",
               colors=["#e74c3c", "#2ecc71", "#95a5a6"], startangle=140)
        ax.set_title("Contributed to Accident?",
                     pad=TITLE_PAD, fontsize=14, fontweight="bold")
        save(fig, "q6_contributed_to_accident")


# ══════════════════════════════════════════════════════════════════════════════
# Q7 — Arrest type distribution
# ══════════════════════════════════════════════════════════════════════════════

def q7_arrest_type(df: pd.DataFrame) -> None:
    print("[Q7]  Arrest type distribution ...")

    if "arrest_type" in df.columns:
        at = df["arrest_type"].value_counts().head(10)
        fig, ax = plt.subplots(figsize=(10, 5))
        sns.barplot(x=at.values, y=at.index, palette=PALETTE, ax=ax)
        ax.set_title("Top 10 Arrest Types", pad=TITLE_PAD, fontsize=14, fontweight="bold")
        ax.set_xlabel("Count")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{int(x):,}"))
        save(fig, "q7_arrest_types")


# ══════════════════════════════════════════════════════════════════════════════
# Summary statistics
# ══════════════════════════════════════════════════════════════════════════════

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "═" * 60)
    print("  📊  SUMMARY STATISTICS")
    print("═" * 60)
    print(f"  Total records           : {len(df):>12,}")

    for col, label in [
        ("accident",        "Accidents                "),
        ("fatal",           "Fatal incidents          "),
        ("personal_injury", "Personal injuries        "),
        ("property_damage", "Property damage          "),
        ("alcohol",         "Alcohol-related          "),
    ]:
        if col in df.columns:
            cnt = df[col].astype(str).str.lower().eq("true").sum()
            pct = cnt / len(df) * 100
            print(f"  {label}: {cnt:>12,}  ({pct:.2f}%)")

    if "violation_type" in df.columns:
        print(f"\n  Top violation type      : {df['violation_type'].value_counts().idxmax()}")
    if "make" in df.columns:
        print(f"  Most common vehicle make: {df['make'].value_counts().idxmax()}")
    if "race" in df.columns:
        print(f"  Most cited race         : {df['race'].value_counts().idxmax()}")
    if "gender" in df.columns:
        print(f"  Most cited gender       : {df['gender'].value_counts().idxmax()}")
    if "stop_hour" in df.columns:
        peak = int(df["stop_hour"].dropna().astype(int).value_counts().idxmax())
        print(f"  Peak violation hour     : {peak:02d}:00")

    print("═" * 60 + "\n")


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    df = load_data()
    print_summary(df)

    print("── Generating EDA charts ──────────────────────────────────")
    q1_top_violations(df)
    q2_top_locations(df)
    q3_demographics(df)
    q4_temporal_patterns(df)
    q5_vehicle_analysis(df)
    q6_severity_analysis(df)
    q7_arrest_type(df)

    print(f"\n✅  EDA complete! All charts saved to: reports/eda_charts/")


if __name__ == "__main__":
    main()
