import streamlit as st
import requests
import pandas as pd
import json
import concurrent.futures
import threading
import plotly.express as px
import plotly.graph_objects as go

# ── Config ────────────────────────────────────────────────────────────────────
FAM_LIST_ID   = 9511991   # FAM-enrolled PRISM members
PRISM_LIST_ID = 9518831   # All enrolled PRISM members (denominator)
CUTOFF_DATE   = pd.Timestamp("2025-12-20")
APRIL_MONTH   = "2026-04"
APRIL_START   = pd.Timestamp("2026-04-01")

# Add monthly targets here as new campaign months are defined
MONTHLY_TARGETS = {
    "2026-04": 200,
}

FAM_FIELDS = ("enrollmentDate", "companyName", "employeeOrDependent", "gender")

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_headers():
    return {"Api-Key": st.secrets["ITERABLE_KEY_DIGBI_HEALTH"]}


def fmt_month(period_str: str) -> str:
    """Convert '2026-04' → 'Apr 2026'"""
    try:
        return pd.Period(period_str, "M").strftime("%b %Y")
    except Exception:
        return period_str


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_list_emails(list_id: int) -> list:
    """Fetch plain-text email list from Iterable."""
    resp = requests.get(
        "https://api.iterable.com/api/lists/getUsers",
        headers=get_headers(),
        params={"listId": list_id},
        stream=True,
        timeout=300,
    )
    resp.raise_for_status()
    emails = []
    for line in resp.iter_lines():
        if line:
            decoded = line.decode("utf-8") if isinstance(line, bytes) else line
            decoded = decoded.strip()
            if decoded:
                try:
                    obj = json.loads(decoded)
                    if obj.get("email"):
                        emails.append(obj["email"])
                except json.JSONDecodeError:
                    emails.append(decoded)
    return emails


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_user_fields(emails: tuple, fields: tuple) -> list:
    """Fetch full profile fields for FAM members (threaded)."""
    headers    = get_headers()
    email_list = list(emails)
    results    = []
    lock       = threading.Lock()
    completed  = [0]

    progress = st.progress(0, text="Loading FAM member profiles…")

    def fetch_one(email):
        try:
            r = requests.get(
                f"https://api.iterable.com/api/users/{requests.utils.quote(email, safe='')}",
                headers=headers,
                timeout=15,
            )
            if r.status_code == 200:
                u = r.json().get("user", {})
                row = {"email": email}
                row.update({k: v for k, v in u.get("dataFields", {}).items() if k in fields})
                return row
        except Exception:
            pass
        return {"email": email}

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_one, e): e for e in email_list}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            with lock:
                completed[0] += 1
                pct = completed[0] / len(email_list)
                progress.progress(
                    pct,
                    text=f"Loading FAM member profiles… {completed[0]:,}/{len(email_list):,}",
                )
    progress.empty()
    return results


@st.cache_data(ttl=1800, show_spinner=False)
def fetch_prism_dates(emails: tuple) -> list:
    """Fetch enrollmentDate for all PRISM members (for monthly comparison charts)."""
    headers    = get_headers()
    email_list = list(emails)
    results    = []
    lock       = threading.Lock()
    completed  = [0]

    progress = st.progress(0, text="Loading PRISM enrollment history…")

    def fetch_one(email):
        try:
            r = requests.get(
                f"https://api.iterable.com/api/users/{requests.utils.quote(email, safe='')}",
                headers=headers,
                timeout=15,
            )
            if r.status_code == 200:
                u = r.json().get("user", {})
                return {
                    "email": email,
                    "enrollmentDate": u.get("dataFields", {}).get("enrollmentDate"),
                }
        except Exception:
            pass
        return {"email": email, "enrollmentDate": None}

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_one, e): e for e in email_list}
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
            with lock:
                completed[0] += 1
                pct = completed[0] / len(email_list)
                progress.progress(
                    pct,
                    text=f"Loading PRISM enrollment history… {completed[0]:,}/{len(email_list):,}",
                )
    progress.empty()
    return results


def parse_dates(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Parse enrollmentDate stored as Unix milliseconds integer."""
    df = df.copy()
    numeric = pd.to_numeric(df[date_col], errors="coerce")
    df["enrollmentDate"] = pd.to_datetime(numeric, unit="ms", errors="coerce")
    df["date"]        = df["enrollmentDate"].dt.normalize()
    df["month"]       = df["enrollmentDate"].dt.to_period("M").astype(str)
    df["month_label"] = df["month"].apply(fmt_month)
    return df


# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PRISM FAM Enrollment Dashboard",
    page_icon="🥗",
    layout="wide",
)

st.title("🥗 PRISM Food As Medicine (FAM) Enrollment")
st.caption("PRISM Members · Digbi Health · Live data via Iterable")

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Controls")
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()
    st.caption("Data cached for 30 min.")

# ── Data fetch ────────────────────────────────────────────────────────────────
with st.spinner("Fetching PRISM member list…"):
    prism_emails       = fetch_list_emails(PRISM_LIST_ID)
    total_prism        = len(prism_emails)

with st.spinner("Fetching FAM enrolled list…"):
    fam_emails         = fetch_list_emails(FAM_LIST_ID)
    fam_enrolled_count = len(fam_emails)

fam_user_data   = fetch_user_fields(tuple(fam_emails), FAM_FIELDS)
prism_date_data = fetch_prism_dates(tuple(prism_emails))

# ── Parse FAM data ────────────────────────────────────────────────────────────
df_enrolled = pd.DataFrame(fam_user_data)

has_date_col = (
    "enrollmentDate" in df_enrolled.columns
    and df_enrolled["enrollmentDate"].notna().any()
)

if has_date_col:
    df_enrolled = parse_dates(df_enrolled, "enrollmentDate")
    df_chart    = df_enrolled[
        df_enrolled["date"].notna() & (df_enrolled["date"] >= CUTOFF_DATE)
    ].copy()
    has_dates = len(df_chart) > 0
else:
    df_chart  = df_enrolled.copy()
    has_dates = False

# ── Parse PRISM data ──────────────────────────────────────────────────────────
df_prism = pd.DataFrame(prism_date_data)
prism_has_dates = (
    "enrollmentDate" in df_prism.columns
    and df_prism["enrollmentDate"].notna().any()
)
if prism_has_dates:
    df_prism       = parse_dates(df_prism, "enrollmentDate")
    df_prism_chart = df_prism[
        df_prism["date"].notna() & (df_prism["date"] >= CUTOFF_DATE)
    ].copy()
else:
    df_prism_chart = pd.DataFrame()

# ── Normalize gender ──────────────────────────────────────────────────────────
if "gender" in df_chart.columns:
    df_chart["gender"] = df_chart["gender"].replace({"M": "Male", "F": "Female"})

# ── KPI calculations ──────────────────────────────────────────────────────────
glp1_enrolled   = max(total_prism - fam_enrolled_count, 0)
enrollment_rate = (fam_enrolled_count / total_prism * 100) if total_prism > 0 else 0.0

today       = pd.Timestamp.today().normalize()
month_start = today.replace(day=1)
month_str   = today.strftime("%Y-%m")
month_label = today.strftime("%b %Y")

# April (fixed campaign target month)
april_target   = MONTHLY_TARGETS.get(APRIL_MONTH, None)
april_enrolled = int((df_chart["date"] >= APRIL_START).sum()) if has_dates else 0
april_velocity = april_enrolled / 30  # April has 30 days
april_pct      = (april_enrolled / april_target * 100) if april_target else None

# Current month
days_passed       = max((today - month_start).days, 1)
days_in_month     = pd.Period(today, "M").days_in_month
month_enrollments = int((df_chart["date"] >= month_start).sum()) if has_dates else 0
current_velocity  = month_enrollments / days_passed
current_target    = MONTHLY_TARGETS.get(month_str, None)
current_pct       = (month_enrollments / current_target * 100) if current_target else None

# ── KPI Row 1 — Overall ───────────────────────────────────────────────────────
st.subheader("Key Metrics")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Enrolled PRISM Members", f"{total_prism:,}")
c2.metric("FAM Enrolled",           f"{fam_enrolled_count:,}")
c3.metric("GLP-1 Enrolled",         f"{glp1_enrolled:,}")
c4.metric("FAM Enrollment %",       f"{enrollment_rate:.1f}%")

# ── April row — campaign target month (always shown) ─────────────────────────
st.divider()
st.caption("📌 April 2026 — Campaign Target Month")
a1, a2, a3, a4 = st.columns(4)
a1.metric("Apr 2026 Target",       f"{april_target:,}" if april_target else "—")
a2.metric("FAM Apr 2026 Enrolled", f"{april_enrolled:,}")
a3.metric("Apr Target % Achieved", f"{april_pct:.1f}%" if april_pct is not None else "—")
a4.metric("FAM Velocity Apr",      f"{april_velocity:.1f} / day")

# ── Current month row (shown only when we've moved past April) ────────────────
if month_str != APRIL_MONTH:
    st.divider()
    st.caption(f"📅 {month_label} — Current Month")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric(f"{today.strftime('%b')} Target",
              f"{current_target:,}" if current_target else "—")
    m2.metric(f"FAM {today.strftime('%b %Y')} Enrolled",
              f"{month_enrollments:,}")
    m3.metric(f"{today.strftime('%b')} Target % Achieved",
              f"{current_pct:.1f}%" if current_pct is not None else "—")
    m4.metric(f"FAM Velocity {today.strftime('%b')}",
              f"{current_velocity:.1f} / day")

# ── FAM Enrollment Detail Tables ─────────────────────────────────────────────
st.divider()
st.subheader("FAM Enrollment Detail Tables")

if not has_dates or len(df_chart) == 0:
    st.info("No enrollment date data available for tables.")
else:
    tbl_day, tbl_month, tbl_company = st.tabs(["📅 By Day", "📆 By Month", "🏢 By Company"])

    # ── By Day ────────────────────────────────────────────────────────────────
    with tbl_day:
        df_this_month = df_chart[df_chart["date"] >= month_start].copy()

        if len(df_this_month) > 0:
            daily_tbl = (
                df_this_month.groupby("date")
                .size()
                .reset_index(name="Enrollments")
                .sort_values("date")
            )
            full_daily = (
                df_chart.groupby("date").size()
                .reset_index(name="cnt").sort_values("date")
            )
            full_daily["cum"] = full_daily["cnt"].cumsum()
            cum_map = full_daily.set_index("date")["cum"].to_dict()

            daily_tbl["Cumulative Total"] = daily_tbl["date"].map(cum_map)
            daily_tbl["Day"]              = daily_tbl["date"].dt.strftime("%A, %b %d")
            daily_tbl = daily_tbl[["Day", "Enrollments", "Cumulative Total"]]

            st.markdown(f"#### 📅 {today.strftime('%B %Y')} Enrollments by Day")
            st.dataframe(daily_tbl, use_container_width=True, hide_index=True)
        else:
            st.info(f"No enrollments recorded yet for {today.strftime('%B %Y')}.")

    # ── By Month ──────────────────────────────────────────────────────────────
    with tbl_month:
        fam_monthly = (
            df_chart.groupby("month").size()
            .reset_index(name="FAM Enrollments").sort_values("month")
        )

        if len(df_prism_chart) > 0:
            prism_monthly = (
                df_prism_chart.groupby("month").size()
                .reset_index(name="Total Enrollments").sort_values("month")
            )
            monthly_tbl = fam_monthly.merge(prism_monthly, on="month", how="left")
            monthly_tbl["Total Enrollments"] = (
                monthly_tbl["Total Enrollments"].fillna(0).astype(int)
            )
            monthly_tbl["FAM %"] = (
                monthly_tbl["FAM Enrollments"]
                / monthly_tbl["Total Enrollments"].replace(0, pd.NA)
                * 100
            ).map(lambda x: f"{x:.1f}%" if pd.notna(x) else "—")
        else:
            monthly_tbl = fam_monthly.copy()
            monthly_tbl["Total Enrollments"] = "—"
            monthly_tbl["FAM %"]             = "—"

        monthly_tbl["Month"] = monthly_tbl["month"].apply(fmt_month)
        monthly_tbl = monthly_tbl[["Month", "FAM Enrollments", "Total Enrollments", "FAM %"]]

        st.markdown("#### 📆 FAM Enrollments by Month")
        st.dataframe(monthly_tbl, use_container_width=True, hide_index=True)

    # ── By Company ────────────────────────────────────────────────────────────
    with tbl_company:
        if "companyName" in df_chart.columns and df_chart["companyName"].notna().any():
            co_tbl = (
                df_chart[df_chart["companyName"].notna()]
                .groupby("companyName").size()
                .reset_index(name="FAM Enrollments")
                .sort_values("FAM Enrollments", ascending=False)
                .rename(columns={"companyName": "Company"})
            )
            st.markdown("#### 🏢 FAM Enrollments by Company")
            st.dataframe(co_tbl, use_container_width=True, hide_index=True)
        else:
            st.info("No company name data available.")

# ── FAM Enrollment Trends & Breakdowns ───────────────────────────────────────
st.divider()
st.subheader("FAM Enrollment Trends & Breakdowns")

if not has_dates or len(df_chart) == 0:
    st.info("No enrollment date data available yet for chart display.")
else:
    # ── FAM vs. Total PRISM monthly bar chart ─────────────────────────────────
    if len(df_prism_chart) > 0:
        fam_m   = df_chart.groupby("month").size().reset_index(name="Enrollments")
        fam_m["Type"] = "FAM Enrollments"

        prism_m = df_prism_chart.groupby("month").size().reset_index(name="Enrollments")
        prism_m["Type"] = "Total PRISM Enrollments"

        combined = pd.concat([prism_m, fam_m], ignore_index=True).sort_values("month")
        combined["Month"] = combined["month"].apply(fmt_month)
        month_order = combined["Month"].unique().tolist()

        fig_compare = px.bar(
            combined,
            x="Month",
            y="Enrollments",
            color="Type",
            barmode="group",
            title="FAM vs. Total PRISM Enrollment Trends",
            labels={"Month": "Month", "Enrollments": "Enrollments"},
            color_discrete_map={
                "Total PRISM Enrollments": "#4C72B0",
                "FAM Enrollments":         "#2E8B57",
            },
            category_orders={"Month": month_order},
        )
        st.plotly_chart(fig_compare, use_container_width=True)

        # ── FAM % of PRISM line chart ──────────────────────────────────────────
        fam_mo   = df_chart.groupby("month").size().reset_index(name="fam_cnt")
        prism_mo = df_prism_chart.groupby("month").size().reset_index(name="prism_cnt")
        pct_df   = fam_mo.merge(prism_mo, on="month", how="left").sort_values("month")
        pct_df["FAM %"] = (
            pct_df["fam_cnt"] / pct_df["prism_cnt"].replace(0, pd.NA) * 100
        ).round(1)
        pct_df["Month"] = pct_df["month"].apply(fmt_month)
        pct_order = pct_df["Month"].tolist()

        fig_pct = px.line(
            pct_df,
            x="Month",
            y="FAM %",
            title="FAM Enrollments as % of Total PRISM Enrollments by Month",
            labels={"Month": "Month", "FAM %": "FAM % of Total Enrollments"},
            markers=True,
            text="FAM %",
            color_discrete_sequence=["#E07B39"],
            category_orders={"Month": pct_order},
        )
        fig_pct.update_traces(texttemplate="%{text:.1f}%", textposition="top center")
        fig_pct.update_yaxes(ticksuffix="%")
        st.plotly_chart(fig_pct, use_container_width=True)

    # ── Employee vs. Dependent + Gender ───────────────────────────────────────
    col_emp, col_gender = st.columns(2)

    with col_emp:
        if "employeeOrDependent" in df_chart.columns and df_chart["employeeOrDependent"].notna().any():
            emp = (
                df_chart[df_chart["employeeOrDependent"].notna()]
                .groupby("employeeOrDependent").size()
                .reset_index(name="enrollments")
            )
            fig_emp = px.pie(
                emp,
                names="employeeOrDependent",
                values="enrollments",
                title="Employee vs. Dependent",
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            fig_emp.update_traces(textinfo="label+percent+value")
            st.plotly_chart(fig_emp, use_container_width=True)
        else:
            st.info("No employee/dependent data available.")

    with col_gender:
        if "gender" in df_chart.columns and df_chart["gender"].notna().any():
            gender = (
                df_chart[df_chart["gender"].notna()]
                .groupby("gender").size()
                .reset_index(name="enrollments")
            )
            fig_gen = px.pie(
                gender,
                names="gender",
                values="enrollments",
                title="FAM Enrollments by Gender",
                color_discrete_sequence=px.colors.qualitative.Safe,
            )
            fig_gen.update_traces(textinfo="label+percent+value")
            st.plotly_chart(fig_gen, use_container_width=True)
        else:
            st.info("No gender data available.")

# ── Raw data expander ─────────────────────────────────────────────────────────
with st.expander("🔍 View Raw Enrollment Data"):
    display_cols = [
        c for c in ["email", "enrollmentDate", "companyName", "employeeOrDependent", "gender"]
        if c in df_chart.columns
    ]
    st.dataframe(
        df_chart[display_cols].sort_values("enrollmentDate", ascending=False)
        if "enrollmentDate" in df_chart.columns
        else df_chart[display_cols],
        use_container_width=True,
    )
    st.caption(f"{len(df_chart):,} records shown (enrollment date ≥ Dec 20, 2025)")
