import time
import redis
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import requests

# PAGE CONFIGURATION

st.set_page_config(
    page_title="OlistIQ — Live Operations Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# REDIS CONNECTION

@st.cache_resource
def get_redis_connection():
    return redis.Redis(host="redis", port=6379, decode_responses=True)

r = get_redis_connection()

# BRAZIL GEOJSON

@st.cache_resource
def get_brazil_geojson():
    urls = [
        "https://raw.githubusercontent.com/giuliano-oliveira/geodata-br-states/main/geojson/br_states.json",
        "https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson"
    ]
    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data and "features" in data and len(data["features"]) > 0:
                    props = data["features"][0]["properties"]
                    return data, props
        except Exception:
            continue
    return None, {}

# DATA FETCHING FUNCTIONS

def fetch_metrics():
    total_orders        = int(r.get("metrics:total_orders") or 0)
    total_revenue       = float(r.get("metrics:total_revenue") or 0)
    total_freight       = float(r.get("metrics:total_freight") or 0)
    score_sum           = float(r.get("metrics:review_score_sum") or 0)
    score_count         = int(r.get("metrics:review_score_count") or 1)
    avg_score           = round(score_sum / score_count, 2)
    avg_order_value     = round(total_revenue / max(total_orders, 1), 2)
    delivery_days_sum   = float(r.get("metrics:delivery_days_sum") or 0)
    delivery_days_count = int(r.get("metrics:delivery_days_count") or 1)
    avg_delivery_days   = round(delivery_days_sum / delivery_days_count, 1)
    weight_sum          = float(r.get("metrics:weight_sum") or 0)
    weight_count        = int(r.get("metrics:weight_count") or 1)
    avg_weight_g        = round(weight_sum / weight_count, 0)
    freight_ratio_sum   = float(r.get("metrics:freight_ratio_sum") or 0)
    freight_ratio_count = int(r.get("metrics:freight_ratio_count") or 1)
    avg_freight_pct     = round((freight_ratio_sum / freight_ratio_count) * 100, 1)

    return {
        "total_orders":       total_orders,
        "total_revenue":      round(total_revenue, 2),
        "total_freight":      round(total_freight, 2),
        "avg_score":          avg_score,
        "avg_order_value":    avg_order_value,
        "avg_delivery_days":  avg_delivery_days,
        "avg_weight_g":       avg_weight_g,
        "avg_freight_pct":    avg_freight_pct
    }


def fetch_counter(prefix):
    keys = r.keys(f"{prefix}:*")
    if not keys:
        return pd.DataFrame(columns=["label", "count"])
    values = r.mget(keys)
    labels = [k.replace(f"{prefix}:", "") for k in keys]
    counts = [int(v) for v in values]
    df = pd.DataFrame({"label": labels, "count": counts})
    return df.sort_values("count", ascending=False).reset_index(drop=True)


def fetch_recent_events(n=15):
    order_ids = r.lrange("recent_events", 0, n - 1)
    events = []
    for oid in order_ids:
        data = r.hgetall(f"event:{oid}")
        if data:
            events.append(data)
    if not events:
        return pd.DataFrame()
    df = pd.DataFrame(events)
    for col_name in ["payment_value", "price", "freight_value",
                     "review_score", "product_photos_qty",
                     "product_weight_g", "payment_installments",
                     "delivery_days"]:
        if col_name in df.columns:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    return df


# STYLING HELPERS

COLORS = {
    "green":  "#4CAF50",
    "blue":   "#2196F3",
    "orange": "#FF9800",
    "red":    "#F44336",
    "purple": "#9C27B0",
    "teal":   "#009688",
    "chart_sequence": [
        "#4CAF50", "#2196F3", "#FF9800", "#F44336",
        "#9C27B0", "#009688", "#FF5722", "#607D8B",
        "#E91E63", "#00BCD4", "#8BC34A", "#FFC107"
    ]
}

LAYOUT_DEFAULTS = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    margin=dict(l=0, r=0, t=10, b=0),
    font=dict(color="white")
)


def metric_card(label, value, suffix="", color=None):
    border_color = color or COLORS["green"]
    st.markdown(
        f"""
        <div style="
            background: #1e1e2e;
            border-radius: 12px;
            padding: 20px 24px;
            border-left: 4px solid {border_color};
            margin-bottom: 8px;
        ">
            <div style="color:#aaa; font-size:13px; margin-bottom:6px;">{label}</div>
            <div style="color:#fff; font-size:28px; font-weight:700;">
                {value}<span style="font-size:16px; color:#aaa;"> {suffix}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )


# BUCKET SORT ORDERS

PRICE_BUCKET_ORDER = [
    "Under R$50", "R$50-100", "R$100-200", "R$200-400", "Above R$400"
]

FREIGHT_BUCKET_ORDER = [
    "Under R$15", "R$15-30", "R$30-50", "Above R$50"
]

# STATE NAME MAP

STATE_NAME_MAP = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins"
}

# DASHBOARD HEADER

st.markdown(
    """
    <div style="text-align:center; padding: 10px 0 24px 0;">
        <h1 style="color:#4CAF50; font-size:36px; margin:0;">
            OlistIQ — Live Operations Dashboard
        </h1>
        <p style="color:#aaa; margin:6px 0 0 0;">
            Real-time order stream · Kafka → Spark Streaming → Redis → Streamlit
        </p>
    </div>
    """,
    unsafe_allow_html=True
)

# AUTO-REFRESH LOOP

REFRESH_SECONDS = 5
placeholder = st.empty()

while True:

    # Fetch all data at start of each cycle
    metrics         = fetch_metrics()
    status_df       = fetch_counter("counters:status")
    payment_df      = fetch_counter("counters:payment")
    category_df     = fetch_counter("counters:category")
    state_df        = fetch_counter("counters:state")
    seller_state_df = fetch_counter("counters:seller_state")
    installment_df  = fetch_counter("counters:installments")
    photos_df       = fetch_counter("counters:photos")
    score_dist_df   = fetch_counter("counters:review_score")
    comment_df      = fetch_counter("counters:review_has_comment")
    price_df        = fetch_counter("counters:price_bucket")
    freight_df      = fetch_counter("counters:freight_bucket")
    city_df         = fetch_counter("counters:customer_city")
    seller_city_df  = fetch_counter("counters:seller_city")
    recent_df       = fetch_recent_events(15)

    with placeholder.container():

        # SECTION 1 — KPI CARDS ROW 1
        st.markdown("### Key Metrics")
        c1, c2, c3, c4, c5 = st.columns(5)

        with c1:
            metric_card("Total Orders", f"{metrics['total_orders']:,}")
        with c2:
            metric_card("Total Revenue", f"R$ {metrics['total_revenue']:,.2f}")
        with c3:
            metric_card("Avg Order Value", f"R$ {metrics['avg_order_value']:,.2f}")
        with c4:
            metric_card("Avg Review Score", f"{metrics['avg_score']}", suffix="/ 5")
        with c5:
            metric_card("Total Freight", f"R$ {metrics['total_freight']:,.2f}")

        # SECTION 1B — KPI CARDS ROW 2
        c6, c7, c8 = st.columns(3)

        with c6:
            metric_card(
                "Avg Delivery Days",
                f"{metrics['avg_delivery_days']}",
                suffix="days",
                color=COLORS["blue"]
            )
        with c7:
            metric_card(
                "Avg Product Weight",
                f"{int(metrics['avg_weight_g']):,}",
                suffix="g",
                color=COLORS["orange"]
            )
        with c8:
            metric_card(
                "Freight as % of Order",
                f"{metrics['avg_freight_pct']}",
                suffix="%",
                color=COLORS["purple"]
            )

        st.markdown("---")

        # SECTION 2 — ORDER STATUS + PAYMENT TYPE
        st.markdown("### Order & Payment Analysis")
        col_left, col_right = st.columns(2)

        with col_left:
            st.markdown("#### Orders by Status")
            if not status_df.empty:
                fig = px.bar(
                    status_df,
                    x="count", y="label",
                    orientation="h",
                    color="label",
                    color_discrete_sequence=COLORS["chart_sequence"],
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    yaxis_title="",
                    xaxis_title="Orders"
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right:
            st.markdown("#### Payment Type Distribution")
            if not payment_df.empty:
                fig = px.pie(
                    payment_df,
                    names="label",
                    values="count",
                    color_discrete_sequence=COLORS["chart_sequence"],
                    hole=0.45
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    legend=dict(font=dict(color="white"))
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 3 — INSTALLMENTS + REVIEW SCORE DISTRIBUTION
        st.markdown("### Payment Installments & Review Score Distribution")
        col_left2, col_right2 = st.columns(2)

        with col_left2:
            st.markdown("#### Credit Card Installment Breakdown")
            if not installment_df.empty:
                installment_df["label_int"] = pd.to_numeric(
                    installment_df["label"], errors="coerce"
                )
                installment_df = installment_df.sort_values(
                    "label_int"
                ).reset_index(drop=True)
                installment_df["label"] = installment_df["label"].astype(str) + "x"

                fig = px.bar(
                    installment_df,
                    x="label", y="count",
                    color="count",
                    color_continuous_scale="Blues",
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    xaxis_title="Installments",
                    yaxis_title="Orders",
                    coloraxis_showscale=False
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right2:
            st.markdown("#### Review Score Distribution")
            if not score_dist_df.empty:
                score_dist_df["label_int"] = pd.to_numeric(
                    score_dist_df["label"], errors="coerce"
                )
                score_dist_df = score_dist_df.sort_values(
                    "label_int"
                ).reset_index(drop=True)
                score_dist_df["label"] = score_dist_df["label"].astype(str) + " ⭐"

                fig = px.bar(
                    score_dist_df,
                    x="label", y="count",
                    color="label",
                    color_discrete_sequence=[
                        "#F44336", "#FF9800", "#FFC107", "#8BC34A", "#4CAF50"
                    ],
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    xaxis_title="Score",
                    yaxis_title="Reviews"
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 4 — TOP CATEGORIES + PRICE DISTRIBUTION
        st.markdown("### Product & Pricing Analysis")
        col_left3, col_right3 = st.columns(2)

        with col_left3:
            st.markdown("#### Top 10 Product Categories (English)")
            if not category_df.empty:
                top_cat = category_df.head(10)
                fig = px.bar(
                    top_cat,
                    x="label", y="count",
                    color="count",
                    color_continuous_scale="Greens",
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    xaxis_title="",
                    yaxis_title="Orders",
                    coloraxis_showscale=False
                )
                fig.update_xaxes(tickangle=45)
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right3:
            st.markdown("#### Order Price Distribution")
            if not price_df.empty:
                price_df["sort_order"] = price_df["label"].apply(
                    lambda x: PRICE_BUCKET_ORDER.index(x)
                    if x in PRICE_BUCKET_ORDER else 99
                )
                price_df = price_df.sort_values(
                    "sort_order"
                ).reset_index(drop=True)

                fig = px.bar(
                    price_df,
                    x="label", y="count",
                    color="label",
                    color_discrete_sequence=COLORS["chart_sequence"],
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    xaxis_title="Price Range",
                    yaxis_title="Orders"
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 5 — PRODUCT PHOTOS + FREIGHT DISTRIBUTION
        st.markdown("### Product Quality & Freight Analysis")
        col_left4, col_right4 = st.columns(2)

        with col_left4:
            st.markdown("#### Product Listing Photos Count")
            if not photos_df.empty:
                photos_df["label_int"] = pd.to_numeric(
                    photos_df["label"], errors="coerce"
                )
                photos_df = photos_df.sort_values(
                    "label_int"
                ).reset_index(drop=True)
                photos_df["label"] = photos_df["label"].astype(str) + " photo(s)"

                fig = px.bar(
                    photos_df,
                    x="label", y="count",
                    color="count",
                    color_continuous_scale="Oranges",
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    xaxis_title="Photos",
                    yaxis_title="Products",
                    coloraxis_showscale=False
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right4:
            st.markdown("#### Freight Cost Distribution")
            if not freight_df.empty:
                freight_df["sort_order"] = freight_df["label"].apply(
                    lambda x: FREIGHT_BUCKET_ORDER.index(x)
                    if x in FREIGHT_BUCKET_ORDER else 99
                )
                freight_df = freight_df.sort_values(
                    "sort_order"
                ).reset_index(drop=True)

                fig = px.pie(
                    freight_df,
                    names="label",
                    values="count",
                    color_discrete_sequence=COLORS["chart_sequence"],
                    hole=0.45
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    legend=dict(font=dict(color="white"))
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 6 — CUSTOMER vs SELLER STATE + REVIEW COMMENT RATE
        st.markdown("### Geographic & Engagement Analysis")
        col_left5, col_right5 = st.columns(2)

        with col_left5:
            st.markdown("#### Customer vs Seller State Comparison (Top 10)")
            if not state_df.empty and not seller_state_df.empty:
                customer_merged = state_df.rename(
                    columns={"count": "customer_orders"}
                )
                seller_merged = seller_state_df.rename(
                    columns={"count": "seller_orders"}
                )
                merged = pd.merge(
                    customer_merged,
                    seller_merged,
                    on="label",
                    how="outer"
                ).fillna(0)
                merged = merged.sort_values(
                    "customer_orders", ascending=False
                ).head(10).reset_index(drop=True)

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    name="Customer Orders",
                    x=merged["label"],
                    y=merged["customer_orders"],
                    marker_color=COLORS["green"]
                ))
                fig.add_trace(go.Bar(
                    name="Seller Orders",
                    x=merged["label"],
                    y=merged["seller_orders"],
                    marker_color=COLORS["blue"]
                ))
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    barmode="group",
                    xaxis_title="State",
                    yaxis_title="Orders",
                    legend=dict(font=dict(color="white"))
                )
                st.plotly_chart(fig, use_container_width=True)

        with col_right5:
            st.markdown("#### Review Comment Rate")
            if not comment_df.empty:
                fig = px.pie(
                    comment_df,
                    names="label",
                    values="count",
                    color_discrete_sequence=[COLORS["green"], COLORS["red"]],
                    hole=0.45
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    legend=dict(font=dict(color="white"))
                )
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 7 — BRAZIL STATE MAP (FIXED WITH GEOJSON)
        st.markdown("### Orders by Brazilian State")

        geojson_data, sample_props = get_brazil_geojson()

        if not state_df.empty and geojson_data is not None:

            # Determine which property key holds the state name
            name_key = "name"
            if sample_props:
                for candidate in ["name", "NOME_UF", "nm_estado", "NM_ESTADO"]:
                    if candidate in sample_props:
                        name_key = candidate
                        break

            map_df = state_df.copy()
            map_df["state_name"] = map_df["label"].map(STATE_NAME_MAP)
            map_df = map_df.dropna(subset=["state_name"])

            fig = px.choropleth(
                map_df,
                geojson=geojson_data,
                locations="state_name",
                featureidkey=f"properties.{name_key}",
                color="count",
                color_continuous_scale="Greens",
                hover_name="label",
                hover_data={"count": True, "state_name": False},
                range_color=[0, map_df["count"].max()]
            )
            fig.update_geos(
                fitbounds="locations",
                visible=False
            )
            fig.update_layout(
                **LAYOUT_DEFAULTS,
                geo=dict(bgcolor="rgba(0,0,0,0)"),
                coloraxis_showscale=True,
                coloraxis_colorbar=dict(
                    title="Orders",
                    tickfont=dict(color="white"),
                    titlefont=dict(color="white")
                ),
                height=450
            )
            st.plotly_chart(fig, use_container_width=True)

        elif state_df.empty:
            st.info("Waiting for state data from the stream...")
        else:
            st.markdown("#### Orders by State (map unavailable — showing bar chart)")
            top_states = state_df.head(15)
            fig = px.bar(
                top_states,
                x="label", y="count",
                color="count",
                color_continuous_scale="Greens",
                text="count"
            )
            fig.update_layout(
                **LAYOUT_DEFAULTS,
                xaxis_title="State",
                yaxis_title="Orders",
                coloraxis_showscale=False
            )
            fig.update_traces(textposition="outside")
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 8 — TOP CUSTOMER CITIES + TOP SELLER CITIES
        st.markdown("### Top Cities Analysis")
        col_left6, col_right6 = st.columns(2)

        with col_left6:
            st.markdown("#### Top 10 Customer Cities")
            if not city_df.empty:
                top_cities = city_df.head(10)
                fig = px.bar(
                    top_cities.sort_values("count", ascending=True),
                    x="count", y="label",
                    orientation="h",
                    color="count",
                    color_continuous_scale="Teal",
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    yaxis_title="",
                    xaxis_title="Orders",
                    coloraxis_showscale=False
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        with col_right6:
            st.markdown("#### Top 10 Seller Cities")
            if not seller_city_df.empty:
                top_seller_cities = seller_city_df.head(10)
                fig = px.bar(
                    top_seller_cities.sort_values("count", ascending=True),
                    x="count", y="label",
                    orientation="h",
                    color="count",
                    color_continuous_scale="Purp",
                    text="count"
                )
                fig.update_layout(
                    **LAYOUT_DEFAULTS,
                    showlegend=False,
                    yaxis_title="",
                    xaxis_title="Orders",
                    coloraxis_showscale=False
                )
                fig.update_traces(textposition="outside")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # SECTION 9 — LIVE EVENT FEED TABLE
        st.markdown("#### Live Event Feed — Last 15 Orders")
        if not recent_df.empty:
            display_cols = [
                "order_id", "order_status", "customer_state",
                "customer_city", "product_category", "payment_type",
                "payment_installments", "payment_value", "price",
                "freight_value", "review_score", "delivery_days",
                "event_timestamp"
            ]
            available = [c for c in display_cols if c in recent_df.columns]
            display_df = recent_df[available].copy()

            if "order_id" in display_df.columns:
                display_df["order_id"] = display_df["order_id"].str[:8] + "..."

            if "event_timestamp" in display_df.columns:
                display_df["event_timestamp"] = pd.to_datetime(
                    display_df["event_timestamp"], errors="coerce"
                ).dt.strftime("%H:%M:%S")

            display_df.columns = [
                c.replace("_", " ").title() for c in display_df.columns
            ]
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Waiting for events from the stream...")

        # FOOTER
        st.markdown(
            f"<p style='text-align:center; color:#555; font-size:12px;'>"
            f"Refreshing every {REFRESH_SECONDS} seconds</p>",
            unsafe_allow_html=True
        )

    time.sleep(REFRESH_SECONDS)