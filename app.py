import streamlit as st
import requests
import pandas as pd
import json
import concurrent.futures
import threading
import plotly.express as px
import plotly.graph_objects as go

# ── Config ────────────────────────────────────────────────────────────────────
FAM_LIST_ID    = 9511991   # FAM-enrolled PRISM members
PRISM_LIST_ID  = 9518831   # All PRISM members (denominator)
CUTOFF_DATE    = pd.Timestamp("2025-12-20")
APRIL_START    = pd.Timestamp("2026-04-01")
APRIL_TARGET   = 200

FIELDS = ("enrollmentDate", "companyName", "employeeOrDependent", "gender")

# ── Helpers ───────────────────────────────────────────────────────────────────
def get_headers():
    return {"Api-Key": st.secrets["ITERABLE_KEY_DIGBI_HEALTH"]}


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
    """Fetch profile fields for each email using threaded GET /users/{email}."""
    headers   = get_headers()
    email_list = list(emails)
    results   = []
    lock      = threading.Lock()
    completed = [0]

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
                row.update(
                    {k: v for k, v in u.get("dataFields", {}).items() if k in fields}
                )
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


def parse_dates(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Parse enrollmentDate stored as Unix milliseconds integer."""
    df = df.copy()
    numeric = pd.to_numeric(df[date_col], errors="coerce")
    df["enrollmentDate"] = pd.to_datetime(numeric, unit="ms", errors="coerce")
    df["date"]  = df["enrollmentDate"].dt.normalize()
    df["month"] = df["enrollmentDate"].dt.to_period("M").astype(str)
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
    prism_emails = fetch_list_emails(PRISM_LIST_ID)
    total_prism  = len(prism_emails)

with st.spinner("Fetching FAM enrolled list…"):
    fam_emails       = fetch_list_emails(FAM_LIST_ID)
    fam_enrolled_count = len(fam_emails)

user_data = fetch_user_fields(tuple(fam_emails), FIELDS)

# ── Parse & filter ────────────────────────────────────────────────────────────
df_enrolled = pd.DataFrame(user_data)

has_date_col = (
    "enrollmentDate" in df_enrolled.columns
    and df_enrolled["enrollmentDate"].notna().any()
)

if has_date_col:
    df_enrolled = parse_dates(df_enrolled, "enrollmentDate")
    df_chart = df_enrolled[
        df_enrolled["date"].notna() & (df_enrolled["date"] >= CUTOFF_DATE)
    ].copy()
    has_dates = len(df_chart) > 0
else:
    df_chart  = df_enrolled.copy()
    has_dates = False

# ── Normalize gender values ───────────────────────────────────────────────────
if "gender" in df_chart.columns:
    df_chart["gender"] = df_chart["gender"].replace({"M": "Male", "F": "Female"})

# ── KPI calculations ──────────────────────────────────────────────────────────
glp1_enrolled   = max(total_prism - fam_enrolled_count, 0)
enrollment_rate = (fam_enrolled_count / total_prism * 100) if total_prism > 0 else 0.0

today       = pd.Timestamp.today().normalize()
month_start = today.replace(day=1)

april_enrolled = int((df_chart["date"] >= APRIL_START).sum()) if has_dates else 0
april_pct      = (april_enrolled / APRIL_TARGET * 100) if APRIL_TARGET > 0 else 0.0

# Velocity
days_passed   = max((today - month_start).days, 1)
days_in_month = pd.Period(today, "M").days_in_month
days_left     = max(days_in_month - today.day, 1)

month_enrollments    = int((df_chart["date"] >= month_start).sum()) if has_dates else 0
current_velocity     = month_enrollments / days_passed
remaining_to_target  = max(APRIL_TARGET - april_enrolled, 0)
expected_velocity    = remaining_to_target / days_left if days_left > 0 else 0.0

# ── KPI Row 1 ─────────────────────────────────────────────────────────────────
st.subheader("Key Metrics")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Enrolled PRISM Members", f"{total_prism:,}")
c2.metric("FAM Enrolled",           f"{fam_enrolled_count:,}")
c3.metric("GLP-1 Enrolled",         f"{glp1_enrolled:,}")
c4.metric("FAM Enrollment %",       f"{enrollment_rate:.1f}%")

# ── KPI Row 2 — April + Velocity ──────────────────────────────────────────────
st.divider()
r2c1, r2c2, r2c3, r2c4, r2c5, r2c6 = st.columns(6)
r2c1.metric("FAM April 2026 Enrolled",   f"{april_enrolled:,}",
            delta=f"{april_enrolled - APRIL_TARGET:+,} vs target")
r2c2.metric("April 2026 Target",         f"{APRIL_TARGET:,}")
r2c3.metric("FAM April Target Progress", f"{april_pct:.1f}%")
r2c4.metric(
    f"{today.strftime('%B')} Enrollment Velocity",
    f"{current_velocity:.1f} / day",
)
r2c5.metric(
    "Velocity Needed to Hit Target",
    f"{expected_velocity:.1f} / day",
)
r2c6.metric("Days Left in April", f"{days_left}")

# ── FAM Enrollment Detail Tables ─────────────────────────────────────────────
st.divider()
st.subheader("FAM Enrollment Detail Tables")

if not has_dates or len(df_chart) == 0:
    st.info("No enrollment date data available for tables.")
else:
    tbl_day, tbl_month = st.tabs(["📅 By Day", "📆 By Month"])

    with tbl_day:
        current_month_start = today.replace(day=1)
        df_this_month = df_chart[df_chart["date"] >= current_month_start].copy()

        if len(df_this_month) > 0:
            daily_tbl = (
                df_this_month.groupby("date")
                .size()
                .reset_index(name="Enrollments")
                .sort_values("date")
            )
            full_daily = (
                df_chart.groupby("date")
                .size()
                .reset_index(name="cnt")
                .sort_values("date")
            )
            full_daily["cum"] = full_daily["cnt"].cumsum()
            cum_map = full_daily.set_index("date")["cum"].to_dict()

            daily_tbl["Cumulative Total"] = daily_tbl["date"].map(cum_map)
            daily_tbl["Day"] = daily_tbl["date"].dt.strftime("%A, %b %d")
            daily_tbl = daily_tbl[["Day", "Enrollments", "Cumulative Total"]]

            st.markdown(f"#### 📅 {today.strftime('%B %Y')} Enrollments by Day")
            st.dataframe(daily_tbl, use_container_width=True, hide_index=True)
        else:
            st.info(f"No enrollments recorded yet for {today.strftime('%B %Y')}.")

    with tbl_month:
        monthly_tbl = (
            df_chart.groupby("month")
            .size()
            .reset_index(name="Enrollments")
            .sort_values("month")
        )
        monthly_tbl["Cumulative Total"] = monthly_tbl["Enrollments"].cumsum()
        monthly_tbl = monthly_tbl.rename(columns={"month": "Month"})

        st.markdown("#### 📆 FAM Enrollments by Month")
        st.dataframe(monthly_tbl, use_container_width=True, hide_index=True)

# ── FAM Enrollment Trends & Breakdowns ───────────────────────────────────────
st.divider()
st.subheader("FAM Enrollment Trends & Breakdowns")

if not has_dates or len(df_chart) == 0:
    st.info("No enrollment date data available yet for chart display.")
else:
    col_emp, col_gender = st.columns(2)

    # ── Employee vs. Dependent ────────────────────────────────────────────────
    with col_emp:
        if "employeeOrDependent" in df_chart.columns and df_chart["employeeOrDependent"].notna().any():
            emp = (
                df_chart[df_chart["employeeOrDependent"].notna()]
                .groupby("employeeOrDependent")
                .size()
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

    # ── By Gender ─────────────────────────────────────────────────────────────
    with col_gender:
        if "gender" in df_chart.columns and df_chart["gender"].notna().any():
            gender = (
                df_chart[df_chart["gender"].notna()]
                .groupby("gender")
                .size()
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
    display_cols = [c for c in ["email", "enrollmentDate", "companyName", "employeeOrDependent", "gender"] if c in df_chart.columns]
    st.dataframe(df_chart[display_cols].sort_values("enrollmentDate", ascending=False) if "enrollmentDate" in df_chart.columns else df_chart[display_cols], use_container_width=True)
    st.caption(f"{len(df_chart):,} records shown (enrollment date ≥ Dec 20, 2025)")
