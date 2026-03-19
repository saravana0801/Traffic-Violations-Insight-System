"""
dashboard.py
------------
Interactive Streamlit dashboard for Traffic Violations Insight System.

Run:
    streamlit run app/dashboard.py
"""

import os
import warnings
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import folium
from streamlit_folium import st_folium

warnings.filterwarnings("ignore")

# ── Page config (MUST be first Streamlit call) ─────────────────────────────────
st.set_page_config(
    page_title="Traffic Violations Insight System",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH  = os.path.join(BASE_DIR, "data", "processed", "cleaned_traffic.csv")

# ── Color palette ──────────────────────────────────────────────────────────────
PRIMARY   = "#2C3E50"
ACCENT    = "#E74C3C"
BLUE      = "#2980B9"
GREEN     = "#27AE60"
ORANGE    = "#E67E22"
PURPLE    = "#8E44AD"
PLOTLY_PALETTE = px.colors.qualitative.Safe

# ══════════════════════════════════════════════════════════════════════════════
# Data Loading (cached)
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="Loading dataset …")
def load_data() -> pd.DataFrame:
    df = pd.read_csv(DATA_PATH, low_memory=False)
    df["date_of_stop"] = pd.to_datetime(df["date_of_stop"], errors="coerce")
    for col in ["stop_hour", "month", "vehicle_year", "year", "latitude", "longitude"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # Normalise boolean columns to actual bool
    bool_cols = [
        "accident", "belts", "personal_injury", "property_damage",
        "fatal", "commercial_license", "hazmat", "commercial_vehicle",
        "alcohol", "work_zone", "search_conducted", "contributed_to_accident",
    ]
    for col in bool_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str).str.strip().str.lower()
                       .map({"true": True, "1": True, "false": False, "0": False})
            )
    return df


# ══════════════════════════════════════════════════════════════════════════════
# Sidebar Filters
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar(df: pd.DataFrame) -> pd.DataFrame:
    # st.sidebar.image(
    #     "https://img.icons8.com/color/96/traffic-light.png", width=60
    # )
    st.sidebar.title("Filters")

    # ── Date range ─────────────────────────────────────────────────────────────
    min_date = df["date_of_stop"].min().date()
    max_date = df["date_of_stop"].max().date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start_date, end_date = date_range
        df = df[
            (df["date_of_stop"].dt.date >= start_date) &
            (df["date_of_stop"].dt.date <= end_date)
        ]

    # ── Sub-Agency ─────────────────────────────────────────────────────────────
    agency_col = "sub_agency" if "sub_agency" in df.columns else "subagency"
    if agency_col in df.columns:
        agencies = sorted(df[agency_col].dropna().unique())
        sel_agency = st.sidebar.multiselect("Sub-Agency", agencies, default=[])
        if sel_agency:
            df = df[df[agency_col].isin(sel_agency)]

    # ── Violation Type ─────────────────────────────────────────────────────────
    if "violation_type" in df.columns:
        vt_options = sorted(df["violation_type"].dropna().unique())
        sel_vt = st.sidebar.multiselect("Violation Type", vt_options, default=[])
        if sel_vt:
            df = df[df["violation_type"].isin(sel_vt)]

    # ── Gender ─────────────────────────────────────────────────────────────────
    if "gender" in df.columns:
        genders = sorted(df["gender"].dropna().unique())
        sel_gender = st.sidebar.multiselect("Gender", genders, default=[])
        if sel_gender:
            df = df[df["gender"].isin(sel_gender)]

    # ── Race ───────────────────────────────────────────────────────────────────
    if "race" in df.columns:
        races = sorted(df["race"].dropna().astype(str).unique())
        sel_race = st.sidebar.multiselect("Race", races, default=[])
        if sel_race:
            df = df[df["race"].isin(sel_race)]

    # ── Vehicle Make ───────────────────────────────────────────────────────────
    if "make" in df.columns:
        top_makes = df["make"].value_counts().head(30).index.tolist()
        sel_make = st.sidebar.multiselect("Vehicle Make (Top 30)", top_makes, default=[])
        if sel_make:
            df = df[df["make"].isin(sel_make)]

    # ── Time of Day ────────────────────────────────────────────────────────────
    if "time_of_day" in df.columns:
        tod_options = ["Morning", "Afternoon", "Evening", "Night"]
        sel_tod = st.sidebar.multiselect("Time of Day", tod_options, default=[])
        if sel_tod:
            df = df[df["time_of_day"].isin(sel_tod)]

    st.sidebar.markdown("---")
    st.sidebar.caption(f"**{len(df):,}** records match current filters")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# KPI Cards
# ══════════════════════════════════════════════════════════════════════════════

def render_kpis(df: pd.DataFrame) -> None:
    st.subheader("📋 Summary KPIs")
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    total      = len(df)
    accidents  = int(df["accident"].sum())        if "accident"        in df.columns else 0
    fatalities = int(df["fatal"].sum())            if "fatal"           in df.columns else 0
    injuries   = int(df["personal_injury"].sum())  if "personal_injury" in df.columns else 0
    alcohol    = int(df["alcohol"].sum())           if "alcohol"         in df.columns else 0

    agency_col = "sub_agency" if "sub_agency" in df.columns else "subagency"
    high_risk  = df[agency_col].value_counts().idxmax() if agency_col in df.columns and total > 0 else "N/A"

    def card(col,label, value, color):
        col.markdown(
            f"""
            <div style="background:{color};padding:14px 10px;border-radius:10px;text-align:center;color:white;">
                <div style="font-size:12px;opacity:0.9">{label}</div>
                <div style="font-size:18px;font-weight:700">{value}</div>                
            </div>
            """,
            unsafe_allow_html=True,
        )

    card(c1,  "Total Violations", f"{total:,}",      PRIMARY)
    card(c2,  "Accidents",         f"{accidents:,}",  ACCENT)
    card(c3,  "Fatalities",        f"{fatalities:,}", "#C0392B")
    card(c4,  "Injuries",          f"{injuries:,}",   ORANGE)
    card(c5,  "Alcohol-Related",   f"{alcohol:,}",    PURPLE)
    card(c6,  "Top Agency",        high_risk,         BLUE)

    st.markdown("<br>", unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 1 — Violations Overview
# ══════════════════════════════════════════════════════════════════════════════

def tab_overview(df: pd.DataFrame) -> None:

    col1, col2 = st.columns(2)

    # Violation type bar
    with col1:
        if "violation_type" in df.columns:
            vc = df["violation_type"].value_counts().reset_index()
            vc.columns = ["Violation Type", "Count"]
            fig = px.bar(vc.head(10), x="Count", y="Violation Type",
                         orientation="h", color="Count",
                         color_continuous_scale="Blues",
                         title="Top 10 Violation Types")
            fig.update_layout(yaxis=dict(autorange="reversed"),
                              coloraxis_showscale=False, height=380)
            st.plotly_chart(fig, use_container_width=True)

    # Violation type pie
    with col2:
        if "violation_type" in df.columns:
            vc2 = df["violation_type"].value_counts().head(6).reset_index()
            vc2.columns = ["Violation Type", "Count"]
            fig2 = px.pie(vc2, names="Violation Type", values="Count",
                          color_discrete_sequence=PLOTLY_PALETTE,
                          title="Violation Type Share")
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            fig2.update_layout(height=380, showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

    # Top descriptions
    if "description" in df.columns:
        vd = df["description"].value_counts().head(15).reset_index()
        vd.columns = ["Description", "Count"]
        fig3 = px.bar(vd, x="Count", y="Description", orientation="h",
                      color="Count", color_continuous_scale="Oranges",
                      title="Top 15 Violation Descriptions")
        fig3.update_layout(yaxis=dict(autorange="reversed"),
                           coloraxis_showscale=False, height=460)
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 2 — Temporal Trends
# ══════════════════════════════════════════════════════════════════════════════

def tab_temporal(df: pd.DataFrame) -> None:

    col1, col2 = st.columns(2)

    # By hour
    with col1:
        if "stop_hour" in df.columns:
            hourly = (
                df["stop_hour"].dropna().astype(int)
                               .value_counts().sort_index()
                               .reset_index()
            )
            hourly.columns = ["Hour", "Count"]
            fig = px.bar(hourly, x="Hour", y="Count",
                         color="Count", color_continuous_scale="Viridis",
                         title="Violations by Hour of Day")
            fig.update_layout(coloraxis_showscale=False, height=350)
            st.plotly_chart(fig, use_container_width=True)

    # By time-of-day bucket
    with col2:
        if "time_of_day" in df.columns:
            tod_order = ["Morning", "Afternoon", "Evening", "Night", "Unknown"]
            tod = (
                df["time_of_day"].value_counts()
                                 .reindex(tod_order)
                                 .dropna()
                                 .reset_index()
            )
            tod.columns = ["Time of Day", "Count"]
            fig2 = px.bar(tod, x="Time of Day", y="Count",
                          color="Time of Day",
                          color_discrete_sequence=PLOTLY_PALETTE,
                          title="Violations by Time-of-Day Bucket")
            fig2.update_layout(showlegend=False, height=350)
            st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    # By day of week
    with col3:
        if "day_of_week" in df.columns:
            day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            dow = (
                df["day_of_week"].value_counts()
                                 .reindex(day_order, fill_value=0)
                                 .reset_index()
            )
            dow.columns = ["Day", "Count"]
            fig3 = px.bar(dow, x="Day", y="Count",
                          color="Count", color_continuous_scale="Teal",
                          title="Violations by Day of Week")
            fig3.update_layout(coloraxis_showscale=False, height=350)
            st.plotly_chart(fig3, use_container_width=True)

    # By month
    with col4:
        if "month" in df.columns:
            month_labels = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                            7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
            monthly = (
                df["month"].dropna().astype(int)
                           .value_counts().sort_index()
                           .reset_index()
            )
            monthly.columns = ["Month", "Count"]
            monthly["Month"] = monthly["Month"].map(month_labels)
            fig4 = px.line(monthly, x="Month", y="Count", markers=True,
                           title="Violations by Month")
            fig4.update_traces(line_color=BLUE, fill="tozeroy", fillcolor="rgba(41,128,185,0.1)")
            fig4.update_layout(height=350)
            st.plotly_chart(fig4, use_container_width=True)

    # Daily trend line
    if "date_of_stop" in df.columns:
        daily = df.groupby(df["date_of_stop"].dt.date).size().reset_index()
        daily.columns = ["Date", "Count"]
        fig5 = px.line(daily, x="Date", y="Count",
                       title="Daily Violation Trend")
        fig5.update_traces(line_color=ACCENT)
        fig5.update_layout(height=350)
        st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 3 — Demographics
# ══════════════════════════════════════════════════════════════════════════════

def tab_demographics(df: pd.DataFrame) -> None:

    col1, col2 = st.columns(2)

    # By race
    with col1:
        if "race" in df.columns:
            rc = df["race"].value_counts().head(10).reset_index()
            rc.columns = ["Race", "Count"]
            fig = px.bar(rc, x="Count", y="Race", orientation="h",
                         color="Count", color_continuous_scale="Purples",
                         title="Violations by Driver Race")
            fig.update_layout(yaxis=dict(autorange="reversed"),
                              coloraxis_showscale=False, height=380)
            st.plotly_chart(fig, use_container_width=True)

    # By gender
    with col2:
        if "gender" in df.columns:
            gc = df["gender"].value_counts().reset_index()
            gc.columns = ["Gender", "Count"]
            fig2 = px.pie(gc, names="Gender", values="Count",
                          color_discrete_sequence=[BLUE, ACCENT, "#95A5A6"],
                          title="Violations by Gender")
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            fig2.update_layout(height=380)
            st.plotly_chart(fig2, use_container_width=True)

    # Race × Violation Type heatmap
    if "race" in df.columns and "violation_type" in df.columns:
        top_races = df["race"].value_counts().head(7).index
        top_vt    = df["violation_type"].value_counts().head(6).index
        sub = df[df["race"].isin(top_races) & df["violation_type"].isin(top_vt)]
        pivot = sub.groupby(["race", "violation_type"]).size().unstack(fill_value=0)
        if not pivot.empty:
            fig3 = px.imshow(
                pivot,
                color_continuous_scale="YlOrRd",
                title="Race × Violation Type Heatmap",
                aspect="auto",
                text_auto=True,
            )
            fig3.update_layout(height=400)
            st.plotly_chart(fig3, use_container_width=True)

    # Gender × accident rate
    if "gender" in df.columns and "accident" in df.columns:
        ga = (
            df[df["gender"].isin(["M","F"])]
            .groupby("gender")
            .agg(total=("gender","count"), accidents=("accident","sum"))
            .reset_index()
        )
        ga["accident_rate"] = (ga["accidents"] / ga["total"] * 100).round(2)
        col3, col4 = st.columns(2)
        with col3:
            fig4 = px.bar(ga, x="gender", y="accident_rate",
                          color="gender", text="accident_rate",
                          color_discrete_sequence=[BLUE, ACCENT],
                          title="Accident Rate by Gender (%)")
            fig4.update_traces(texttemplate="%{text:.2f}%", textposition="outside")
            fig4.update_layout(showlegend=False, height=340)
            st.plotly_chart(fig4, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 4 — Vehicle Analysis
# ══════════════════════════════════════════════════════════════════════════════

def tab_vehicles(df: pd.DataFrame) -> None:

    col1, col2 = st.columns(2)

    # Top makes
    with col1:
        if "make" in df.columns:
            mk = df["make"].value_counts().head(15).reset_index()
            mk.columns = ["Make", "Count"]
            fig = px.bar(mk, x="Count", y="Make", orientation="h",
                         color="Count", color_continuous_scale="Blues",
                         title="Top 15 Vehicle Makes")
            fig.update_layout(yaxis=dict(autorange="reversed"),
                              coloraxis_showscale=False, height=460)
            st.plotly_chart(fig, use_container_width=True)

    # Vehicle type
    with col2:
        vt_col = "vehicletype" if "vehicletype" in df.columns else "vehicle_type"
        if vt_col in df.columns:
            vt = df[vt_col].value_counts().head(10).reset_index()
            vt.columns = ["Vehicle Type", "Count"]
            fig2 = px.pie(vt, names="Vehicle Type", values="Count",
                          color_discrete_sequence=PLOTLY_PALETTE,
                          title="Vehicle Type Distribution")
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            fig2.update_layout(height=460, showlegend=True)
            st.plotly_chart(fig2, use_container_width=True)

    # Vehicle year histogram
    yr_col = "vehicle_year" if "vehicle_year" in df.columns else "year"
    if yr_col in df.columns:
        yr = df[yr_col].dropna()
        fig3 = px.histogram(yr, x=yr, nbins=40,
                            color_discrete_sequence=[BLUE],
                            title="Distribution of Vehicle Manufacture Year",
                            labels={yr_col: "Year"})
        fig3.update_layout(height=340, showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    # Top makes in accidents
    if "make" in df.columns and "accident" in df.columns:
        acc_makes = (
            df[df["accident"] == True]["make"]
            .value_counts().head(10).reset_index()
        )
        acc_makes.columns = ["Make", "Accident Count"]
        fig4 = px.bar(acc_makes, x="Accident Count", y="Make", orientation="h",
                      color="Accident Count", color_continuous_scale="Reds",
                      title="Top 10 Vehicle Makes Involved in Accidents")
        fig4.update_layout(yaxis=dict(autorange="reversed"),
                           coloraxis_showscale=False, height=380)
        st.plotly_chart(fig4, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 5 — Severity & Safety
# ══════════════════════════════════════════════════════════════════════════════

def tab_severity(df: pd.DataFrame) -> None:

    # Severity indicator bar
    severity_cols = {
        "accident":        "Accident",
        "personal_injury": "Personal Injury",
        "property_damage": "Property Damage",
        "fatal":           "Fatal",
        "alcohol":         "Alcohol",
        "belts":           "Seatbelt",
        "hazmat":          "HAZMAT",
    }
    present = {label: int(df[col].sum())
               for col, label in severity_cols.items()
               if col in df.columns}

    if present:
        sev_df = pd.DataFrame(list(present.items()), columns=["Indicator","Count"])
        sev_df.sort_values("Count", ascending=False, inplace=True)
        fig = px.bar(sev_df, x="Count", y="Indicator", orientation="h",
                     color="Count", color_continuous_scale="Reds",
                     title="Severity Indicators (Incident Count)")
        fig.update_layout(yaxis=dict(autorange="reversed"),
                          coloraxis_showscale=False, height=380)
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    # Contributed to accident pie
    with col1:
        if "contributed_to_accident" in df.columns:
            ca = df["contributed_to_accident"].astype(str).str.lower().value_counts().reset_index()
            ca.columns = ["Contributed", "Count"]
            fig2 = px.pie(ca, names="Contributed", values="Count",
                          color_discrete_sequence=["#E74C3C","#2ECC71","#95A5A6"],
                          title="Contributed to Accident?")
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            fig2.update_layout(height=340)
            st.plotly_chart(fig2, use_container_width=True)

    # Alcohol by hour
    with col2:
        if "alcohol" in df.columns and "stop_hour" in df.columns:
            alc = (
                df[df["alcohol"] == True]["stop_hour"]
                .dropna().astype(int)
                .value_counts().sort_index().reset_index()
            )
            alc.columns = ["Hour", "Alcohol Violations"]
            fig3 = px.bar(alc, x="Hour", y="Alcohol Violations",
                          color="Alcohol Violations",
                          color_continuous_scale="Purples",
                          title="Alcohol-Related Violations by Hour")
            fig3.update_layout(coloraxis_showscale=False, height=340)
            st.plotly_chart(fig3, use_container_width=True)

    # Severity by sub-agency table
    agency_col = "sub_agency" if "sub_agency" in df.columns else "subagency"
    if agency_col in df.columns:
        agg_cols = {agency_col: "Sub-Agency"}
        grp = df.groupby(agency_col).agg(
            Total        = (agency_col, "count"),
            Accidents    = ("accident",        lambda x: x.sum() if "accident"        in df.columns else 0),
            Fatalities   = ("fatal",           lambda x: x.sum() if "fatal"           in df.columns else 0),
            Injuries     = ("personal_injury", lambda x: x.sum() if "personal_injury" in df.columns else 0),
        ).reset_index().sort_values("Accidents", ascending=False).head(15)
        grp.rename(columns={agency_col: "Sub-Agency"}, inplace=True)
        st.markdown("#### 🏢 Severity by Sub-Agency (Top 15)")
        st.dataframe(grp, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 6 — Geographic Heatmap
# ══════════════════════════════════════════════════════════════════════════════

def tab_map(df: pd.DataFrame) -> None:

    if "latitude" not in df.columns or "longitude" not in df.columns:
        st.warning("No GPS coordinate columns found.")
        return

    map_df = df[["latitude", "longitude"]].dropna()
    map_df = map_df[
        map_df["latitude"].between(24, 50) &
        map_df["longitude"].between(-125, -65)
    ]

    if map_df.empty:
        st.warning("No valid GPS coordinates available with current filters.")
        return

    # Limit points for performance
    sample = map_df.sample(min(len(map_df), 15_000), random_state=42)

    st.markdown(f"Showing **{len(sample):,}** of **{len(map_df):,}** geo-tagged incidents")

    # Choose map type
    map_type = st.radio("Map Type", ["Scatter Map", "Folium Heatmap"], horizontal=True)

    if map_type == "Scatter Map":
        fig = px.scatter_mapbox(
            sample,
            lat="latitude",
            lon="longitude",
            zoom=10,
            height=550,
            color_discrete_sequence=[ACCENT],
            opacity=0.4,
            title="Incident Locations",
        )
        fig.update_layout(mapbox_style="open-street-map", margin={"r":0,"t":30,"l":0,"b":0})
        st.plotly_chart(fig, use_container_width=True)

    else:
        # Folium heatmap
        center_lat = float(sample["latitude"].mean())
        center_lon = float(sample["longitude"].mean())
        m = folium.Map(location=[center_lat, center_lon], zoom_start=10, tiles="CartoDB positron")

        from folium.plugins import HeatMap
        heat_data = sample[["latitude","longitude"]].values.tolist()
        HeatMap(heat_data, radius=8, blur=10, max_zoom=13).add_to(m)

        st_folium(m, width=None, height=520)


# ══════════════════════════════════════════════════════════════════════════════
# Tab 7 — Data Explorer
# ══════════════════════════════════════════════════════════════════════════════

def tab_explorer(df: pd.DataFrame) -> None:

    st.markdown(f"### 🔎 Filtered Data — {len(df):,} records")

    # Column selector
    all_cols = df.columns.tolist()
    default_cols = [c for c in [
        "date_of_stop","time_of_stop","sub_agency","subagency","description",
        "location","violation_type","race","gender","make","model",
        "accident","fatal","arrest_type",
    ] if c in all_cols]
    sel_cols = st.multiselect("Select columns to display", all_cols, default=default_cols)

    if sel_cols:
        page_size = st.select_slider("Rows per page", options=[25, 50, 100, 200, 500], value=50)
        total_pages = max(1, (len(df) - 1) // page_size + 1)
        page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, step=1)
        start = (page - 1) * page_size
        end   = start + page_size
        st.dataframe(
            df[sel_cols].iloc[start:end].reset_index(drop=True),
            use_container_width=True,
            height=480,
        )
        st.caption(f"Page {page} of {total_pages}  |  {len(df):,} total rows")

    # Download filtered data
    @st.cache_data
    def to_csv(d):
        return d.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="⬇️  Download Filtered Data as CSV",
        data=to_csv(df[sel_cols] if sel_cols else df),
        file_name="filtered_traffic_violations.csv",
        mime="text/csv",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Main App
# ══════════════════════════════════════════════════════════════════════════════

def main():
    # ── Header ─────────────────────────────────────────────────────────────────
    st.markdown(
        """
        <div style="background:linear-gradient(90deg,#2C3E50,#E74C3C);
                    padding:20px 28px;border-radius:12px;margin-bottom:20px">
            <h1 style="color:white;margin:0;font-size:2rem">
                🚦 Traffic Violations Insight System
            </h1>
            <p style="color:#ECF0F1;margin:4px 0 0;font-size:0.95rem">
                EDA • Data Cleaning • Interactive Dashboard &nbsp;|&nbsp;
                Montgomery County Traffic Data
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Load & check data ──────────────────────────────────────────────────────
    if not os.path.exists(DATA_PATH):
        st.error(
            "⚠️  Cleaned dataset not found at:\n\n"
            f"`{DATA_PATH}`\n\n"
            "Please run **`python src/preprocessing.py`** first."
        )
        st.stop()

    df_raw = load_data()

    # ── Sidebar filters ────────────────────────────────────────────────────────
    df = render_sidebar(df_raw)

    # ── KPI row ────────────────────────────────────────────────────────────────
    render_kpis(df)

    # ── Tabs ───────────────────────────────────────────────────────────────────
    tabs = st.tabs([
        "Overview",
        "Temporal Trends",
        "Demographics",
        "Vehicles",
        "Severity & Safety",
        "Geographic Map",
        "Data Explorer",
    ])

    with tabs[0]: tab_overview(df)
    with tabs[1]: tab_temporal(df)
    with tabs[2]: tab_demographics(df)
    with tabs[3]: tab_vehicles(df)
    with tabs[4]: tab_severity(df)
    with tabs[5]: tab_map(df)
    with tabs[6]: tab_explorer(df)

    # ── Footer ─────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.caption("Traffic Violations Insight System · GUVI AI/ML Project · Saravana Karthikeyan · 2026")


if __name__ == "__main__":
    main()
