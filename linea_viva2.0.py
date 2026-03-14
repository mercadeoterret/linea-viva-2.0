"""
Línea Viva v7 — Inventario Inteligente para Térret
Shopify Admin API (REST + GraphQL) directo — sin Google Sheets para inventario.
OAuth Shopify integrado: si no hay token en secrets, la app misma hace el flujo.
"""

import math
import uuid
import urllib.parse
import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime, timedelta, timezone

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Línea Viva · Térret",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CONSTANTES ───────────────────────────────────────────────────────────────
LEAD_TIME_DIAS = 30
UMBRAL_BS      = 25
DIAS_OBJETIVO  = 60
MULTIPLO       = 6
API_VERSION    = "2024-01"

# ─── ESTILOS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

html, body, [data-testid="stAppViewContainer"] {
    background: #F5F0E8 !important;
    color: #1A1A14 !important;
    font-family: 'DM Sans', sans-serif;
}
[data-testid="stAppViewContainer"] > .main { background: #F5F0E8; }
[data-testid="stHeader"] { background: #F5F0E8 !important; border-bottom: 1px solid #D4CFC4; }
section[data-testid="stSidebar"] {
    background: #EDEAE0 !important;
    border-right: 1px solid #D4CFC4 !important;
}
section[data-testid="stSidebar"] .stButton > button {
    background: transparent !important;
    color: #1A1A14 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    border: none !important;
    border-radius: 6px !important;
    padding: 9px 10px !important;
    text-align: left !important;
    width: 100%;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: rgba(45,106,79,0.08) !important;
}
[data-testid="stMetric"] {
    background: #EDEAE0;
    border: 1px solid #D4CFC4;
    border-radius: 6px;
    padding: 10px 14px;
}
[data-testid="stMetricValue"] {
    font-family: 'Bebas Neue', sans-serif !important;
    font-size: 1.8rem !important;
    color: #2D6A4F !important;
}
[data-testid="stMetricLabel"] {
    font-size: 9px !important;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    color: #6B6456 !important;
}
.stButton > button {
    background: #2D6A4F !important;
    color: #F5F0E8 !important;
    font-family: 'Bebas Neue', sans-serif !important;
    font-size: 13px !important;
    letter-spacing: 2px !important;
    border: none !important;
    border-radius: 4px !important;
    padding: 8px 16px !important;
    width: 100%;
}
.stButton > button:hover { opacity: 0.85 !important; }
.stTextInput input, .stNumberInput input, .stDateInput input {
    background: #EDEAE0 !important;
    border: 1px solid #D4CFC4 !important;
    color: #1A1A14 !important;
    border-radius: 4px !important;
    font-size: 13px !important;
}
hr { border-color: #D4CFC4 !important; }
</style>
""", unsafe_allow_html=True)


# ─── SEGMENTACIÓN ─────────────────────────────────────────────────────────────

def calcular_estado(stock, ventas60d, dias_inv):
    try:
        s   = float(stock)
        v   = float(ventas60d)
        cob = float(dias_inv) if str(dias_inv).lower() not in ("inf", "nan", "") else 9999
    except Exception:
        return "HUECO"
    if s == 0 and v == 0:
        return "HUECO"
    if v <= 3 and cob > 90:
        return "LIQUIDAR"
    if (cob <= LEAD_TIME_DIAS and v > 3) or (s == 0 and v > 0):
        return "REPROGRAMAR"
    if v >= 25:
        return "ESTRELLA" if cob <= 120 else "SOBRESTOCK"
    if v >= 10:
        return "ALTA_ROTACION" if cob <= 120 else "SOBRESTOCK"
    if v >= 4:
        return "SALUDABLE" if cob <= 90 else "MONITOREAR"
    return "MONITOREAR"


ESTADOS = {
    "REPROGRAMAR":   {"icon": "⚡", "label": "Reprogramar",    "color": "#FF3B30", "desc": "Cobertura ≤ 30 días con ventas activas, o quiebre. Pedir ya."},
    "ESTRELLA":      {"icon": "⭐", "label": "Estrella",       "color": "#2D6A4F", "desc": "Ventas ≥ 25 en 60d. Best seller — nunca dejar sin stock."},
    "ALTA_ROTACION": {"icon": "🔥", "label": "Alta Rotación",  "color": "#FFB800", "desc": "Ventas ≥ 10 en 60d. Monitorear de cerca."},
    "SOBRESTOCK":    {"icon": "🔴", "label": "Sobrestock",     "color": "#FF6B35", "desc": "Cobertura > 120 días. Pausar pedidos."},
    "SALUDABLE":     {"icon": "✅", "label": "Saludable",      "color": "#00C853", "desc": "Ventas 4-9, cobertura 31-90d. Stock equilibrado."},
    "MONITOREAR":    {"icon": "👁",  "label": "Monitorear",    "color": "#4488FF", "desc": "Ventas 4-9, cobertura > 90d. Revisar próximo ciclo."},
    "LIQUIDAR":      {"icon": "📦", "label": "Liquidar",       "color": "#FF9500", "desc": "Ventas ≤ 3 y cobertura > 90d. Precio especial o retiro."},
    "HUECO":         {"icon": "⚪", "label": "Hueco",          "color": "#B8B0A4", "desc": "Stock 0 y ventas 0. Posiblemente descontinuado."},
}

ORDEN_SIDEBAR = ["REPROGRAMAR", "ESTRELLA", "ALTA_ROTACION", "SOBRESTOCK", "SALUDABLE", "MONITOREAR", "LIQUIDAR", "HUECO"]


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def color_estado(estado):
    return ESTADOS.get(estado, {}).get("color", "#B8B0A4")


def sugerir_cantidad(stock, ventas60d, dias_inv, estado):
    try:
        s = float(stock)
        v = float(ventas60d)
        d = float(dias_inv) if str(dias_inv).lower() not in ("inf", "nan", "") else 9999
    except Exception:
        return 0, "Sin datos"
    if estado in ("LIQUIDAR", "HUECO"):
        return 0, "No reponer"
    if v == 0:
        return 0, "Sin ventas"
    ventas_dia       = v / 60.0
    stock_al_recibir = max(0.0, s - ventas_dia * LEAD_TIME_DIAS)
    necesarias       = (ventas_dia * DIAS_OBJETIVO) - stock_al_recibir
    if necesarias <= 0:
        return 0, f"Stock OK — {int(d)}d"
    cantidad = int(math.ceil(necesarias / MULTIPLO) * MULTIPLO)
    cantidad = max(MULTIPLO, cantidad)
    dias_con = int((s + cantidad) / ventas_dia) if ventas_dia > 0 else 9999
    return cantidad, f"{dias_con}d con pedido"


def fmt_pesos(valor):
    if valor >= 1_000_000:
        return f"${valor/1_000_000:.1f}M"
    if valor >= 1_000:
        return f"${valor:,.0f}"
    return f"${valor:.0f}"


PLOT_BASE = dict(
    paper_bgcolor="#EDEAE0",
    plot_bgcolor="#EDEAE0",
    font=dict(color="#1A1A14", family="DM Sans"),
    margin=dict(t=10, b=10, l=10, r=10),
)


# ─── SHOPIFY OAUTH ────────────────────────────────────────────────────────────

def shopify_get_token():
    """
    1) Secret SHOPIFY_ACCESS_TOKEN → úsalo directo.
    2) session_state["shopify_token"] → ya hicimos OAuth esta sesión.
    3) ?code=... en query params → intercambiar por token.
    4) Nada → mostrar botón de autorización.
    """
    token = st.secrets.get("SHOPIFY_ACCESS_TOKEN", "")
    if token:
        return token

    if st.session_state.get("shopify_token"):
        return st.session_state["shopify_token"]

    shop          = st.secrets["TIENDA_URL"]
    client_id     = st.secrets["SHOPIFY_CLIENT_ID"]
    client_secret = st.secrets["SHOPIFY_CLIENT_SECRET"]
    redirect_uri  = st.secrets["REDIRECT_URI"]

    params = st.query_params
    code   = params.get("code", "")
    state  = params.get("state", "")

    if code and state == "lv7":
        with st.spinner("Conectando con Shopify..."):
            resp = requests.post(
                f"https://{shop}/admin/oauth/access_token",
                json={"client_id": client_id, "client_secret": client_secret, "code": code},
                timeout=15,
            )
        if resp.status_code == 200:
            tok = resp.json().get("access_token", "")
            if tok:
                st.session_state["shopify_token"] = tok
                st.query_params.clear()
                st.success(
                    f"✅ Shopify conectado. Copia este token en tus Streamlit secrets como "
                    f"`SHOPIFY_ACCESS_TOKEN`:\n\n`{tok}`"
                )
                st.rerun()
        st.error("Error al obtener token. Intenta de nuevo.")
        st.query_params.clear()
        st.stop()

    scopes   = "read_products,read_inventory,read_locations,read_orders"
    auth_url = (
        f"https://{shop}/admin/oauth/authorize"
        f"?client_id={client_id}"
        f"&scope={scopes}"
        f"&redirect_uri={urllib.parse.quote(redirect_uri, safe='')}"
        f"&state=lv7"
    )

    st.markdown(
        "<div style='max-width:420px;margin:80px auto;text-align:center;'>"
        "<div style='background:#2D6A4F;width:56px;height:56px;border-radius:10px;"
        "display:inline-flex;align-items:center;justify-content:center;"
        "font-family:Bebas Neue,sans-serif;font-size:26px;color:#F5F0E8;"
        "margin-bottom:20px;'>LV</div>"
        "<div style='font-family:Bebas Neue,sans-serif;font-size:32px;letter-spacing:3px;"
        "color:#1A1A14;margin-bottom:4px;'>LÍNEA VIVA</div>"
        "<div style='font-size:10px;color:#6B6456;letter-spacing:2px;"
        "text-transform:uppercase;margin-bottom:32px;'>Térret · Inventario</div>"
        "<div style='font-size:13px;color:#6B6456;margin-bottom:24px;'>"
        "Conecta tu tienda Shopify para continuar.</div>"
        "</div>",
        unsafe_allow_html=True,
    )
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.link_button("🛍 CONECTAR SHOPIFY", auth_url, use_container_width=True)
    st.stop()


# ─── GOOGLE AUTH ──────────────────────────────────────────────────────────────

def check_google_login():
    """Login por contrasena — sin OAuth, sin redirects, sin perdida de session_state."""
    if st.session_state.get("logged_in"):
        return

    # Si viene callback de Shopify, dejar pasar sin pedir login
    if st.query_params.get("state", "") == "lv7":
        return

    app_password = st.secrets.get("APP_PASSWORD", "")

    st.markdown(
        "<div style='max-width:380px;margin:80px auto;text-align:center;'>"
        "<div style='background:#2D6A4F;width:56px;height:56px;border-radius:10px;"
        "display:inline-flex;align-items:center;justify-content:center;"
        "font-family:Bebas Neue,sans-serif;font-size:26px;color:#F5F0E8;"
        "margin-bottom:20px;'>LV</div>"
        "<div style='font-family:Bebas Neue,sans-serif;font-size:32px;letter-spacing:3px;"
        "color:#1A1A14;margin-bottom:4px;'>LINEA VIVA</div>"
        "<div style='font-size:10px;color:#6B6456;letter-spacing:2px;"
        "text-transform:uppercase;margin-bottom:40px;'>Terret · Inventario</div>"
        "</div>",
        unsafe_allow_html=True,
    )

    _, col, _ = st.columns([1, 2, 1])
    with col:
        pwd = st.text_input("Contrasena", type="password", key="login_pwd",
                            placeholder="Ingresa la contrasena")
        if st.button("ENTRAR", key="btn_login"):
            if pwd == app_password:
                st.session_state.logged_in = True
                st.session_state.user_name = "Terret"
                st.rerun()
            else:
                st.error("Contrasena incorrecta.")
    st.stop()


# ─── SHOPIFY API HELPERS ──────────────────────────────────────────────────────

def _headers(token):
    return {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}


def _shop():
    return st.secrets["TIENDA_URL"]


def rest_get(token, endpoint, params=None):
    url  = f"https://{_shop()}/admin/api/{API_VERSION}/{endpoint}"
    resp = requests.get(url, headers=_headers(token), params=params or {}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def rest_paginated(token, endpoint, key, params=None):
    url     = f"https://{_shop()}/admin/api/{API_VERSION}/{endpoint}"
    results = []
    p       = dict(params or {})
    p.setdefault("limit", 250)
    while url:
        resp = requests.get(url, headers=_headers(token), params=p, timeout=30)
        resp.raise_for_status()
        results.extend(resp.json().get(key, []))
        link = resp.headers.get("Link", "")
        url  = None
        p    = {}
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split(";")[0].strip().strip("<>")
                    break
    return results


def graphql_query(token, query, variables=None):
    url  = f"https://{_shop()}/admin/api/{API_VERSION}/graphql.json"
    resp = requests.post(
        url, headers=_headers(token),
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ─── CARGA DE DATOS ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def cargar_locations(_token):
    return rest_get(_token, "locations.json").get("locations", [])


@st.cache_data(ttl=300)
def cargar_productos(_token):
    GQL = """
    query($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id title productType
            variants(first: 100) {
              edges {
                node {
                  id title sku price
                  inventoryItem {
                    id
                    unitCost { amount }
                  }
                }
              }
            }
          }
        }
      }
    }
    """
    productos = []
    cursor    = None
    while True:
        data = graphql_query(_token, GQL, {"cursor": cursor}).get("data", {}).get("products", {})
        for edge in data.get("edges", []):
            node    = edge["node"]
            prod_id = node["id"].split("/")[-1]
            vars_   = []
            for ve in node.get("variants", {}).get("edges", []):
                v       = ve["node"]
                v_id    = v["id"].split("/")[-1]
                inv     = v.get("inventoryItem") or {}
                cost    = float((inv.get("unitCost") or {}).get("amount", 0) or 0)
                inv_id  = inv.get("id", "").split("/")[-1] if inv.get("id") else ""
                vars_.append({
                    "variant_id":       v_id,
                    "variant_title":    v["title"],
                    "sku":              v.get("sku", ""),
                    "price":            float(v.get("price", 0) or 0),
                    "cost":             cost,
                    "inventory_item_id": inv_id,
                })
            productos.append({
                "product_id":   prod_id,
                "title":        node["title"],
                "product_type": node.get("productType", "Sin tipo") or "Sin tipo",
                "variants":     vars_,
            })
        if not data.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = data["pageInfo"]["endCursor"]
    return productos


@st.cache_data(ttl=300)
def cargar_stock(_token, _productos):
    """Consulta inventory_levels por lotes de 50 inventory_item_ids."""
    all_iids = list({
        var["inventory_item_id"]
        for prod in _productos
        for var in prod["variants"]
        if var["inventory_item_id"]
    })
    stock_map = {}
    batch_size = 50
    for i in range(0, len(all_iids), batch_size):
        batch = all_iids[i:i + batch_size]
        niveles = rest_paginated(
            _token, "inventory_levels.json", "inventory_levels",
            {"inventory_item_ids": ",".join(batch), "limit": 250},
        )
        for n in niveles:
            iid = str(n["inventory_item_id"])
            lid = str(n["location_id"])
            qty = max(0, int(n.get("available", 0) or 0))
            if iid not in stock_map:
                stock_map[iid] = {}
            stock_map[iid][lid] = qty
    return stock_map


@st.cache_data(ttl=3600)
def cargar_ventas_60d(_token):
    desde = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders = rest_paginated(
        _token, "orders.json", "orders",
        {"status": "any", "created_at_min": desde, "fields": "id,line_items", "limit": 250},
    )
    ventas = {}
    for order in orders:
        for item in order.get("line_items", []):
            vid = str(item.get("variant_id", ""))
            if vid and vid != "None":
                ventas[vid] = ventas.get(vid, 0) + int(item.get("quantity", 0))
    return ventas


@st.cache_data(ttl=3600)
def cargar_ventas_rango(_token, dias):
    desde = (datetime.now(timezone.utc) - timedelta(days=dias)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders = rest_paginated(
        _token, "orders.json", "orders",
        {"status": "any", "created_at_min": desde,
         "fields": "id,created_at,total_price,line_items", "limit": 250},
    )
    rows = []
    for order in orders:
        fecha = order.get("created_at", "")[:10]
        for item in order.get("line_items", []):
            qty = int(item.get("quantity", 0))
            prc = float(item.get("price", 0) or 0)
            rows.append({
                "fecha":    fecha,
                "producto": item.get("title", ""),
                "variante": item.get("variant_title", ""),
                "sku":      item.get("sku", ""),
                "cantidad": qty,
                "precio":   prc,
                "total":    qty * prc,
            })
    return pd.DataFrame(rows)


def construir_df(productos, stock_map, ventas_map, locations):
    loc_id_to_name = {str(loc["id"]): loc["name"] for loc in locations}
    rows = []
    for prod in productos:
        for var in prod["variants"]:
            iid        = var["inventory_item_id"]
            vid        = var["variant_id"]
            loc_stocks = stock_map.get(iid, {})
            stock_total = sum(loc_stocks.values())
            ventas60d   = ventas_map.get(vid, 0)
            dias_inv    = round(stock_total / (ventas60d / 60), 1) if ventas60d > 0 else 9999

            row = {
                "Producto":    prod["title"],
                "Tipo":        prod["product_type"],
                "Variante":    var["variant_title"],
                "SKU":         var["sku"],
                "Precio Venta": var["price"],
                "Costo":       var["cost"],
                "Stock":       stock_total,
                "Ventas60d":   ventas60d,
                "DiasInv_n":   dias_inv,
                "_variant_id": vid,
                "_inv_item_id": iid,
            }
            for loc_id, loc_name in loc_id_to_name.items():
                row[f"Stock_{loc_name}"] = loc_stocks.get(loc_id, 0)
            rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["_estado"]      = df.apply(lambda r: calcular_estado(r["Stock"], r["Ventas60d"], r["DiasInv_n"]), axis=1)
    df["_bs"]          = df["Ventas60d"] >= UMBRAL_BS
    df["_valor_costo"] = df["Stock"] * df["Costo"]
    df["_valor_venta"] = df["Stock"] * df["Precio Venta"]
    return df


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

def render_sidebar(conteos):
    with st.sidebar:
        st.markdown(
            "<div style='padding:16px 4px 14px 4px;border-bottom:1px solid #D4CFC4;margin-bottom:10px;'>"
            "<div style='display:flex;align-items:center;gap:10px;'>"
            "<div style='background:#2D6A4F;width:30px;height:30px;border-radius:4px;"
            "display:flex;align-items:center;justify-content:center;"
            "font-family:Bebas Neue,sans-serif;font-size:15px;color:#F5F0E8;flex-shrink:0;'>LV</div>"
            "<div>"
            "<div style='font-family:Bebas Neue,sans-serif;font-size:16px;letter-spacing:2px;"
            "color:#1A1A14;line-height:1;'>LÍNEA VIVA</div>"
            "<div style='font-size:9px;color:#6B6456;letter-spacing:1px;text-transform:uppercase;'>"
            "Térret · Inventario</div>"
            "</div></div></div>",
            unsafe_allow_html=True,
        )

        for nav_id, label in [
            ("DASHBOARD",  "📊  Dashboard"),
            ("VENTAS",     "📈  Ventas"),
            ("ROTACION",   "🔄  Rotación"),
            ("TENDENCIAS", "📉  Tendencias"),
        ]:
            if st.button(label, key=f"nav_{nav_id}"):
                st.session_state.vista = nav_id
                st.rerun()

        st.markdown(
            "<hr style='border-color:#D4CFC4;margin:6px 0;'>"
            "<div style='font-size:9px;color:#B8B0A4;letter-spacing:1.5px;"
            "text-transform:uppercase;padding:6px 4px 4px 4px;'>Inventario</div>",
            unsafe_allow_html=True,
        )

        for estado in ORDEN_SIDEBAR:
            cfg = ESTADOS[estado]
            cnt = conteos.get(estado, 0)
            if st.button(f"{cfg['icon']}  {cfg['label']}   {cnt}", key=f"nav_{estado}"):
                st.session_state.vista = estado
                st.rerun()

        st.markdown("<hr style='border-color:#D4CFC4;margin:6px 0;'>", unsafe_allow_html=True)

        if st.button("🔄  Refrescar datos", key="btn_refresh"):
            st.cache_data.clear()
            st.rerun()

        user = st.session_state.get("user_name", "")
        if user:
            st.markdown(
                f"<div style='font-size:10px;color:#B8B0A4;padding:8px 4px 2px;'>{user}</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            f"<div style='font-size:9px;color:#D4CFC4;padding:0 4px;'>"
            f"{datetime.now().strftime('%d/%m/%Y %H:%M')}</div>",
            unsafe_allow_html=True,
        )


# ─── MÓDULO 0: DASHBOARD ──────────────────────────────────────────────────────

def vista_dashboard(df, locations):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:4px;'>DASHBOARD</div>"
        f"<div style='font-size:11px;color:#6B6456;letter-spacing:1px;"
        f"text-transform:uppercase;margin-bottom:20px;'>"
        f"Visión general · {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>",
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("Sin datos. Verifica la conexión con Shopify.")
        return

    loc_names = [loc["name"] for loc in locations]
    loc_cols  = [c for c in df.columns if c.startswith("Stock_")]

    df_view = df.copy()
    if loc_cols:
        sel_loc = st.selectbox("📍 Sucursal", ["Todas"] + loc_names, key="dash_loc")
        if sel_loc != "Todas":
            col_loc = f"Stock_{sel_loc}"
            if col_loc in df_view.columns:
                df_view["Stock"]     = df_view[col_loc].clip(lower=0)
                df_view["DiasInv_n"] = df_view.apply(
                    lambda r: round(r["Stock"] / (r["Ventas60d"] / 60), 1) if r["Ventas60d"] > 0 else 9999, axis=1
                )
                df_view["_estado"]      = df_view.apply(lambda r: calcular_estado(r["Stock"], r["Ventas60d"], r["DiasInv_n"]), axis=1)
                df_view["_valor_costo"] = df_view["Stock"] * df_view["Costo"]
                df_view["_valor_venta"] = df_view["Stock"] * df_view["Precio Venta"]

    tiene_costos  = df_view["Costo"].sum() > 0
    tiene_precios = df_view["Precio Venta"].sum() > 0

    total_skus  = len(df_view)
    total_prods = df_view["Producto"].nunique()
    total_stock = int(df_view["Stock"].sum())
    reprog_n    = int(df_view[df_view["_estado"] == "REPROGRAMAR"]["Producto"].nunique())
    vc          = df_view["_valor_costo"].sum()
    vv          = df_view["_valor_venta"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("SKUs totales",      total_skus)
    with c2: st.metric("Productos",         total_prods)
    with c3: st.metric("Unidades en stock", f"{total_stock:,}")
    with c4: st.metric("A reprogramar",     reprog_n)

    if tiene_costos or tiene_precios:
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Valor inventario (costo)", fmt_pesos(vc) if vc else "—")
        with c2: st.metric("Valor inventario (venta)", fmt_pesos(vv) if vv else "—")
        with c3:
            mg = ((vv - vc) / vc * 100) if vc > 0 else 0
            st.metric("Margen potencial", f"{mg:.1f}%" if mg else "—")

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

    # Pastel + Críticos
    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>SEGMENTOS</div>",
            unsafe_allow_html=True,
        )
        seg = df_view.groupby("_estado")["Producto"].nunique().reset_index()
        seg.columns = ["Estado", "N"]
        seg = seg[seg["N"] > 0]

        fig_pie = go.Figure(go.Pie(
            labels=[ESTADOS.get(e, {}).get("label", e) for e in seg["Estado"]],
            values=seg["N"],
            hole=0.55,
            marker=dict(colors=[color_estado(e) for e in seg["Estado"]], line=dict(color="#F5F0E8", width=2)),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate="<b>%{label}</b><br>%{value} productos<br>%{percent}<extra></extra>",
        ))
        fig_pie.update_layout(
            **PLOT_BASE, height=280, showlegend=False,
            annotations=[dict(text=f"<b>{total_prods}</b><br>productos",
                              x=0.5, y=0.5, font_size=15, showarrow=False,
                              font=dict(color="#1A1A14"))],
        )
        st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})

    with col_r:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>STOCK CRÍTICO — TOP 10</div>",
            unsafe_allow_html=True,
        )
        criticos = (
            df_view[df_view["_estado"] == "REPROGRAMAR"]
            .groupby("Producto")
            .agg(dias_min=("DiasInv_n", "min"), stock_total=("Stock", "sum"))
            .reset_index()
            .sort_values("dias_min")
            .head(10)
        )
        if criticos.empty:
            st.markdown(
                "<div style='text-align:center;padding:60px;color:#6B6456;'>Sin productos críticos 🎉</div>",
                unsafe_allow_html=True,
            )
        else:
            fig_crit = go.Figure(go.Bar(
                x=criticos["dias_min"], y=criticos["Producto"], orientation="h",
                marker=dict(color=criticos["dias_min"],
                            colorscale=[[0, "#FF3B30"], [1, "#FFB800"]], showscale=False),
                text=criticos["dias_min"].apply(lambda x: f"{int(x)}d"),
                textposition="outside", textfont=dict(size=10),
                hovertemplate="<b>%{y}</b><br>%{x:.0f} días<extra></extra>",
            ))
            fig_crit.update_layout(
                **PLOT_BASE, height=310,
                margin=dict(t=10, b=10, l=180, r=60),
                xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False, showticklabels=False,
                           range=[0, max(criticos["dias_min"].max() * 1.35, 35)]),
                yaxis=dict(showgrid=False, tickfont=dict(size=10), automargin=True),
                shapes=[dict(type="line", x0=LEAD_TIME_DIAS, x1=LEAD_TIME_DIAS,
                             y0=-0.5, y1=len(criticos) - 0.5,
                             line=dict(color="#FF3B30", width=1, dash="dot"))],
                annotations=[dict(x=LEAD_TIME_DIAS, y=len(criticos) - 0.5,
                                  text="Lead time", showarrow=False,
                                  font=dict(size=8, color="#CC2200"), xanchor="left")],
            )
            st.plotly_chart(fig_crit, use_container_width=True, config={"displayModeBar": False})

    # Top ventas + Por categoría
    col_l2, col_r2 = st.columns(2)
    with col_l2:
        tc1, tc2, tc3 = st.columns([3, 1, 1])
        with tc1:
            st.markdown(
                "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
                "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>TOP VENTAS 60D</div>",
                unsafe_allow_html=True,
            )
        with tc2:
            n_top = st.select_slider("", options=[10, 15, 20, 30, 50], value=10,
                                     key="top_n", label_visibility="collapsed")
        with tc3:
            por_sku = st.toggle("SKU", key="top_sku", value=False)

        if por_sku:
            top_data = df_view.sort_values("Ventas60d", ascending=True).tail(n_top)
            y_v = (top_data["SKU"] + " · " + top_data["Variante"].str[:14]).tolist()
            x_v = top_data["Ventas60d"].tolist()
            c_v = [color_estado(e) for e in top_data["_estado"]]
        else:
            top_data = (
                df_view.groupby("Producto")
                .agg(Ventas60d=("Ventas60d", "sum"), _estado=("_estado", "first"))
                .reset_index()
                .sort_values("Ventas60d", ascending=True)
                .tail(n_top)
            )
            y_v = top_data["Producto"].tolist()
            x_v = top_data["Ventas60d"].tolist()
            c_v = [color_estado(e) for e in top_data["_estado"]]

        fig_top = go.Figure(go.Bar(
            x=x_v, y=y_v, orientation="h",
            marker=dict(color=c_v),
            text=[f"{int(v)} u" for v in x_v], textposition="outside",
            textfont=dict(size=10),
            hovertemplate="<b>%{y}</b><br>%{x} u<extra></extra>",
        ))
        fig_top.update_layout(
            **PLOT_BASE, height=max(320, n_top * 32),
            margin=dict(t=10, b=10, l=220, r=70),
            xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, tickfont=dict(size=10), automargin=True),
        )
        st.plotly_chart(fig_top, use_container_width=True, config={"displayModeBar": False})

    with col_r2:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>STOCK POR CATEGORÍA</div>",
            unsafe_allow_html=True,
        )
        por_tipo = (
            df_view.groupby("Tipo")
            .agg(stock=("Stock", "sum"), valor_costo=("_valor_costo", "sum"))
            .reset_index()
            .sort_values("stock", ascending=True)
        )
        x_cat   = por_tipo["valor_costo"] if tiene_costos else por_tipo["stock"]
        txt_cat = [fmt_pesos(v) for v in x_cat] if tiene_costos else [f"{int(v)} u" for v in x_cat]

        fig_cat = go.Figure(go.Bar(
            x=x_cat, y=por_tipo["Tipo"].str[:22], orientation="h",
            marker=dict(color="#4488FF", opacity=0.85),
            text=txt_cat, textposition="outside", textfont=dict(size=9),
            hovertemplate="<b>%{y}</b><br>%{text}<extra></extra>",
        ))
        fig_cat.update_layout(
            **PLOT_BASE, height=300,
            margin=dict(t=10, b=10, l=10, r=80),
            xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, tickfont=dict(size=9)),
        )
        st.plotly_chart(fig_cat, use_container_width=True, config={"displayModeBar": False})

    # Valor por categoría
    if tiene_costos or tiene_precios:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin:8px 0;'>VALOR DE INVENTARIO POR CATEGORÍA</div>",
            unsafe_allow_html=True,
        )
        pv = (
            df_view.groupby("Tipo")
            .agg(vc=("_valor_costo", "sum"), vv=("_valor_venta", "sum"))
            .reset_index()
            .sort_values("vv", ascending=True)
        )
        cats = pv["Tipo"].tolist()
        fig_val = go.Figure()
        fig_val.add_trace(go.Bar(
            name="Precio venta", x=pv["vv"], y=cats, orientation="h",
            marker=dict(color="#2D6A4F", opacity=0.85),
            text=[fmt_pesos(v) for v in pv["vv"]], textposition="outside",
            textfont=dict(size=9, color="#2D6A4F"),
            hovertemplate="<b>%{y}</b><br>Venta: $%{x:,.0f}<extra></extra>",
        ))
        fig_val.add_trace(go.Bar(
            name="Costo", x=pv["vc"], y=cats, orientation="h",
            marker=dict(color="#4488FF", opacity=0.9),
            text=[fmt_pesos(v) for v in pv["vc"]], textposition="inside",
            textfont=dict(size=8),
            hovertemplate="<b>%{y}</b><br>Costo: $%{x:,.0f}<extra></extra>",
        ))
        fig_val.update_layout(
            barmode="overlay", **PLOT_BASE,
            height=max(320, len(cats) * 38),
            margin=dict(t=30, b=20, l=160, r=90),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0,
                        font=dict(size=10), bgcolor="rgba(0,0,0,0)", traceorder="reversed"),
            xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False,
                       tickprefix="$", tickformat=",.0f", tickfont=dict(size=9)),
            yaxis=dict(showgrid=False, tickfont=dict(size=10), automargin=False,
                       categoryorder="array", categoryarray=cats),
        )
        st.plotly_chart(fig_val, use_container_width=True, config={"displayModeBar": False})

    # Tabla resumen
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin:8px 0;'>RESUMEN POR SEGMENTO</div>",
        unsafe_allow_html=True,
    )
    resumen = []
    for estado in ORDEN_SIDEBAR:
        cfg = ESTADOS[estado]
        sub = df_view[df_view["_estado"] == estado]
        if sub.empty:
            continue
        row = {
            "Segmento":    cfg["icon"] + " " + cfg["label"],
            "Productos":   sub["Producto"].nunique(),
            "SKUs":        len(sub),
            "Stock total": int(sub["Stock"].sum()),
            "Ventas 60d":  int(sub["Ventas60d"].sum()),
        }
        if tiene_costos:
            row["Valor costo"] = fmt_pesos(sub["_valor_costo"].sum())
        if tiene_precios:
            row["Valor venta"] = fmt_pesos(sub["_valor_venta"].sum())
        resumen.append(row)
    st.dataframe(pd.DataFrame(resumen), use_container_width=True, hide_index=True)


# ─── MÓDULO 1: INVENTARIO ─────────────────────────────────────────────────────

def vista_inventario(df, estado, locations):
    cfg   = ESTADOS[estado]
    color = cfg["color"]

    st.markdown(
        f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-left:4px solid {color};"
        "border-radius:8px;padding:14px 18px;margin-bottom:20px;"
        "display:flex;align-items:center;gap:14px;'>"
        f"<div style='font-size:24px;'>{cfg['icon']}</div>"
        "<div>"
        f"<div style='font-family:Bebas Neue,sans-serif;font-size:20px;letter-spacing:2px;color:{color};'>"
        f"{cfg['label'].upper()}</div>"
        f"<div style='font-size:12px;color:#6B6456;margin-top:2px;'>{cfg['desc']}</div>"
        "</div></div>",
        unsafe_allow_html=True,
    )

    sub = df[df["_estado"] == estado].copy()

    if sub.empty:
        st.markdown(
            f"<div style='text-align:center;padding:60px;color:#6B6456;'>"
            f"<div style='font-size:36px;margin-bottom:12px;'>{cfg['icon']}</div>"
            "<div style='font-family:Bebas Neue,sans-serif;font-size:18px;letter-spacing:2px;'>"
            "Sin productos en este estado</div></div>",
            unsafe_allow_html=True,
        )
        return

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("SKUs",       len(sub))
    with c2: st.metric("Productos",  sub["Producto"].nunique())
    with c3: st.metric("Categorías", sub["Tipo"].nunique())

    fb1, fb2 = st.columns([3, 2])
    with fb1:
        buscar = st.text_input("Buscar", placeholder="Buscar producto...", label_visibility="collapsed",
                               key=f"buscar_{estado}")
    with fb2:
        tipos_disp = sorted(sub["Tipo"].dropna().unique().tolist())
        tipo_sel   = st.selectbox("Categoría", ["Todas"] + tipos_disp,
                                  label_visibility="collapsed", key=f"tipo_{estado}")

    loc_cols  = [c for c in sub.columns if c.startswith("Stock_")]
    loc_names = [c.replace("Stock_", "") for c in loc_cols]
    sel_loc   = "Total"
    if loc_cols:
        sel_loc = st.selectbox("📍 Ver stock de", ["Total"] + loc_names, key=f"loc_{estado}")

    if tipo_sel != "Todas":
        sub = sub[sub["Tipo"] == tipo_sel]
    if buscar:
        sub = sub[sub["Producto"].str.contains(buscar, case=False, na=False)]

    if sub.empty:
        st.info("Sin resultados.")
        return

    mostrar_form = estado in ("REPROGRAMAR", "ESTRELLA", "ALTA_ROTACION")

    for tipo, dt in sub.groupby("Tipo", sort=False):
        st.markdown(
            "<div style='font-family:DM Mono,monospace;font-size:9px;letter-spacing:3px;"
            "color:#B8B0A4;text-transform:uppercase;padding:20px 0 6px 0;"
            "border-bottom:1px solid #D4CFC4;margin-bottom:8px;'>"
            f"{tipo.upper()} · {dt['Producto'].nunique()} productos"
            "</div>",
            unsafe_allow_html=True,
        )
        for prod, gp in dt.groupby("Producto", sort=False):
            gp    = gp.copy().sort_values("Variante")
            n     = len(gp)
            es_bs = bool(gp["_bs"].any())
            bs_tag = " · ⭐ BS" if es_bs else ""

            st.markdown(
                f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
                f"border-left:3px solid {color};"
                "border-radius:8px 8px 0 0;padding:11px 14px;"
                "display:flex;align-items:center;gap:10px;'>"
                f"<div style='font-weight:600;font-size:14px;flex:1;'>{prod.upper()}</div>"
                f"<div style='font-size:11px;color:#6B6456;'>{n} talla{'s' if n > 1 else ''}{bs_tag}</div>"
                "</div>"
                f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
                f"border-left:3px solid {color};"
                "display:grid;grid-template-columns:2fr 1fr 1fr 1.2fr 1fr;"
                "gap:8px;padding:5px 14px;"
                "font-size:9px;color:#6B6456;letter-spacing:1.5px;text-transform:uppercase;"
                "font-family:DM Mono,monospace;'>"
                "<div>VARIANTE</div><div>STOCK</div><div>DÍAS INV.</div><div>VENTAS 60D</div><div>SUGERIDO</div>"
                "</div>",
                unsafe_allow_html=True,
            )

            for _, row in gp.iterrows():
                stock_v = int(row.get(f"Stock_{sel_loc}", row["Stock"]) if sel_loc != "Total" else row["Stock"])
                dias_n  = float(row["DiasInv_n"])
                dias_str = str(int(dias_n)) if dias_n < 9999 else "∞"
                sug, _   = sugerir_cantidad(row["Stock"], row["Ventas60d"], dias_n, estado)

                st.markdown(
                    f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
                    f"border-left:3px solid {color};"
                    "display:grid;grid-template-columns:2fr 1fr 1fr 1.2fr 1fr;"
                    "gap:8px;padding:8px 14px;border-top:1px solid #D4CFC4;"
                    "align-items:center;font-size:13px;'>"
                    f"<div style='font-weight:500;'>{row['Variante']}</div>"
                    f"<div style='font-family:DM Mono,monospace;color:#6B6456;font-size:12px;'>{stock_v} u</div>"
                    f"<div style='font-family:Bebas Neue,sans-serif;font-size:22px;line-height:1;"
                    f"color:{color};'>{dias_str}</div>"
                    f"<div style='font-size:12px;color:#6B6456;'>{int(row['Ventas60d'])} u</div>"
                    f"<div style='font-size:11px;color:#2D6A4F;font-family:DM Mono,monospace;'>"
                    f"{'↑ ' + str(sug) + ' u' if sug > 0 else '—'}</div>"
                    "</div>",
                    unsafe_allow_html=True,
                )

            st.markdown(
                f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
                f"border-left:3px solid {color};"
                "border-radius:0 0 8px 8px;height:6px;'></div>"
                "<div style='height:8px;'></div>",
                unsafe_allow_html=True,
            )

            if mostrar_form:
                sug_prod, sug_lbl = sugerir_cantidad(
                    int(gp["Stock"].sum()), int(gp["Ventas60d"].sum()),
                    float(gp["DiasInv_n"].min()), estado,
                )
                with st.expander(f"📋 Programar orden — {prod}", expanded=False):
                    pf1, pf2, pf3, pf4 = st.columns([2, 2, 2, 2])
                    _uk = uuid.uuid4().hex[:6]
                    with pf1:
                        cant = st.number_input(
                            "Cantidad total", min_value=MULTIPLO,
                            value=max(MULTIPLO, sug_prod), step=MULTIPLO,
                            key=f"cant_{_uk}",
                        )
                    with pf2:
                        fecha_def = (datetime.today() + timedelta(days=LEAD_TIME_DIAS)).date()
                        fecha = st.date_input("Fecha límite", value=fecha_def, key=f"fecha_{_uk}")
                    with pf3:
                        notas = st.text_input("Notas", placeholder="Opcional", key=f"notas_{_uk}")
                    with pf4:
                        st.write("")
                        if st.button("PROGRAMAR", key=f"btn_{_uk}"):
                            st.success(f"Orden registrada — {prod} · {cant} u · entrega {fecha}")

            st.markdown("<div style='height:4px;'></div>", unsafe_allow_html=True)


# ─── MÓDULO 2: VENTAS ─────────────────────────────────────────────────────────

def vista_ventas(token):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:16px;'>VENTAS</div>",
        unsafe_allow_html=True,
    )

    rangos   = {"7 días": 7, "30 días": 30, "60 días": 60, "90 días": 90, "365 días": 365}
    sel_rango = st.selectbox("Período", list(rangos.keys()), index=1)
    dias_sel  = rangos[sel_rango]

    with st.spinner("Cargando ventas..."):
        df_v = cargar_ventas_rango(token, dias_sel)

    if df_v.empty:
        st.info("Sin ventas en el período.")
        return

    df_v["fecha"] = pd.to_datetime(df_v["fecha"])
    tot   = df_v["total"].sum()
    unids = int(df_v["cantidad"].sum())

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Ventas totales",        fmt_pesos(tot))
    with c2: st.metric("Unidades vendidas",      f"{unids:,}")
    with c3: st.metric("Ticket promedio unidad", fmt_pesos(tot / unids) if unids else "—")

    st.markdown("<div style='height:12px;'></div>", unsafe_allow_html=True)

    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>EVOLUCIÓN DIARIA</div>",
        unsafe_allow_html=True,
    )
    evol = df_v.groupby("fecha").agg(total=("total", "sum")).reset_index()
    fig_evol = go.Figure(go.Scatter(
        x=evol["fecha"], y=evol["total"],
        mode="lines", fill="tozeroy",
        line=dict(color="#2D6A4F", width=2),
        fillcolor="rgba(45,106,79,0.12)",
        hovertemplate="<b>%{x|%d/%m}</b><br>$%{y:,.0f}<extra></extra>",
    ))
    fig_evol.update_layout(
        **PLOT_BASE, height=240,
        margin=dict(t=10, b=30, l=70, r=20),
        xaxis=dict(showgrid=False, tickformat="%d/%m"),
        yaxis=dict(showgrid=True, gridcolor="#D4CFC4",
                   tickprefix="$", tickformat=",.0f", tickfont=dict(size=9)),
    )
    st.plotly_chart(fig_evol, use_container_width=True, config={"displayModeBar": False})

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>TOP PRODUCTOS</div>",
            unsafe_allow_html=True,
        )
        tp = (
            df_v.groupby("producto")
            .agg(total=("total", "sum"), unidades=("cantidad", "sum"))
            .reset_index()
            .sort_values("total", ascending=True)
            .tail(15)
        )
        fig_tp = go.Figure(go.Bar(
            x=tp["total"], y=tp["producto"], orientation="h",
            marker=dict(color="#2D6A4F", opacity=0.85),
            text=[fmt_pesos(v) for v in tp["total"]], textposition="outside",
            textfont=dict(size=9),
            hovertemplate="<b>%{y}</b><br>%{text}<extra></extra>",
        ))
        fig_tp.update_layout(
            **PLOT_BASE, height=400,
            margin=dict(t=10, b=10, l=220, r=80),
            xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, tickfont=dict(size=10), automargin=True),
        )
        st.plotly_chart(fig_tp, use_container_width=True, config={"displayModeBar": False})

    with col_r:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>DETALLE POR SKU</div>",
            unsafe_allow_html=True,
        )
        det = (
            df_v.groupby(["producto", "sku", "variante"])
            .agg(unidades=("cantidad", "sum"), total=("total", "sum"))
            .reset_index()
            .sort_values("unidades", ascending=False)
        )
        det["Valor"] = det["total"].apply(fmt_pesos)
        st.dataframe(
            det[["producto", "sku", "variante", "unidades", "Valor"]].rename(columns={
                "producto": "Producto", "sku": "SKU", "variante": "Variante", "unidades": "Unidades",
            }),
            use_container_width=True, hide_index=True,
        )


# ─── MÓDULO 3: ROTACIÓN ───────────────────────────────────────────────────────

def vista_rotacion(df):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:4px;'>ROTACIÓN DE INVENTARIO</div>"
        "<div style='font-size:11px;color:#6B6456;letter-spacing:1px;"
        "text-transform:uppercase;margin-bottom:20px;'>"
        "Convierte capital inmovilizado en reposición de best sellers</div>",
        unsafe_allow_html=True,
    )
    if df.empty:
        st.warning("Sin datos.")
        return

    liq = df[df["_estado"] == "LIQUIDAR"].copy()
    rep = df[df["_estado"].isin(["REPROGRAMAR", "ESTRELLA"])].copy()

    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>PRODUCTOS A LIQUIDAR</div>",
        unsafe_allow_html=True,
    )
    desc_pct = st.slider("Descuento de liquidación (%)", 10, 60, 30, 5, key="desc_liq")
    factor   = 1 - desc_pct / 100

    capital_total = 0.0
    if liq.empty:
        st.info("Sin productos en estado LIQUIDAR actualmente.")
    else:
        liq_ag = liq.groupby("Producto").agg(
            stock=("Stock", "sum"),
            precio=("Precio Venta", "mean"),
            costo=("Costo", "mean"),
        ).reset_index()
        liq_ag["valor_costo"]      = liq_ag["stock"] * liq_ag["costo"]
        liq_ag["precio_liq"]       = liq_ag["precio"] * factor
        liq_ag["valor_liquidacion"] = liq_ag["stock"] * liq_ag["precio_liq"]
        capital_total = liq_ag["valor_liquidacion"].sum()

        liq_ag["Precio liq."] = liq_ag["precio_liq"].apply(fmt_pesos)
        liq_ag["Valor costo"] = liq_ag["valor_costo"].apply(fmt_pesos)
        liq_ag["Capital est."] = liq_ag["valor_liquidacion"].apply(fmt_pesos)
        st.dataframe(
            liq_ag[["Producto", "stock", "Precio liq.", "Valor costo", "Capital est."]].rename(
                columns={"stock": "Stock"}),
            use_container_width=True, hide_index=True,
        )
        st.metric("💰 Capital disponible (est.)", fmt_pesos(capital_total))

    st.markdown("<hr style='border-color:#D4CFC4;margin:20px 0;'>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>CALCULADORA DE REPOSICIÓN</div>",
        unsafe_allow_html=True,
    )

    presupuesto = st.number_input(
        "Presupuesto disponible ($COP)",
        min_value=0, value=int(capital_total), step=100_000, key="presupuesto_rot",
    )

    if rep.empty:
        st.info("Sin productos en REPROGRAMAR o ESTRELLA.")
        return

    rep_ag = rep.groupby("Producto").agg(
        costo=("Costo", "mean"),
        ventas=("Ventas60d", "sum"),
        stock=("Stock", "sum"),
        dias=("DiasInv_n", "min"),
    ).reset_index()
    rep_ag = rep_ag[rep_ag["costo"] > 0].sort_values("ventas", ascending=False)
    rep_ag["sug_unids"]  = rep_ag.apply(
        lambda r: sugerir_cantidad(r["stock"], r["ventas"], r["dias"], "REPROGRAMAR")[0], axis=1
    )

    presupuesto_rest = float(presupuesto)
    rep_ag["unids_posibles"] = 0
    rep_ag["costo_real"]     = 0.0

    for idx, row in rep_ag.iterrows():
        if presupuesto_rest <= 0 or row["costo"] <= 0:
            continue
        max_u = int(presupuesto_rest / row["costo"])
        unids = min(max_u, row["sug_unids"])
        unids = (unids // MULTIPLO) * MULTIPLO
        rep_ag.at[idx, "unids_posibles"] = unids
        rep_ag.at[idx, "costo_real"]     = unids * row["costo"]
        presupuesto_rest -= unids * row["costo"]

    rep_ag["Costo unit."]    = rep_ag["costo"].apply(fmt_pesos)
    rep_ag["Sugerido (u)"]   = rep_ag["sug_unids"].astype(int)
    rep_ag["Posibles (u)"]   = rep_ag["unids_posibles"].astype(int)
    rep_ag["Costo posibles"] = rep_ag["costo_real"].apply(fmt_pesos)
    st.dataframe(
        rep_ag[["Producto", "Costo unit.", "Sugerido (u)", "Posibles (u)", "Costo posibles"]],
        use_container_width=True, hide_index=True,
    )
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Total a invertir",     fmt_pesos(rep_ag["costo_real"].sum()))
    with c2: st.metric("Presupuesto restante", fmt_pesos(float(presupuesto) - rep_ag["costo_real"].sum()))
    with c3: st.metric("Unidades a reponer",   int(rep_ag["unids_posibles"].sum()))


# ─── MÓDULO 4: TENDENCIAS ─────────────────────────────────────────────────────

def vista_tendencias(token):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:16px;'>TENDENCIAS</div>",
        unsafe_allow_html=True,
    )

    with st.spinner("Cargando 90 días de ventas..."):
        df_t = cargar_ventas_rango(token, 90)

    if df_t.empty:
        st.info("Sin datos de ventas.")
        return

    df_t["fecha"] = pd.to_datetime(df_t["fecha"])
    corte = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

    rec = df_t[df_t["fecha"] >= corte].groupby("producto")["cantidad"].sum()
    ant = df_t[df_t["fecha"] <  corte].groupby("producto")["cantidad"].sum()

    comp = pd.DataFrame({"reciente": rec, "anterior": ant}).fillna(0)
    comp["delta"] = comp["reciente"] - comp["anterior"]
    comp["pct"]   = comp.apply(
        lambda r: (r["delta"] / r["anterior"] * 100) if r["anterior"] > 0 else 100.0, axis=1
    )
    comp = comp.sort_values("pct", ascending=False).reset_index()
    comp.columns = ["Producto", "Últimos 30d", "30d anteriores", "Δ u", "Δ %"]

    col_l, col_r = st.columns(2)
    with col_l:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#2D6A4F;margin-bottom:6px;'>📈 CRECIENDO</div>",
            unsafe_allow_html=True,
        )
        cr = comp[comp["Δ %"] > 0].head(10).copy()
        cr["Δ %"] = cr["Δ %"].apply(lambda x: f"+{x:.0f}%")
        st.dataframe(cr[["Producto", "Últimos 30d", "Δ %"]], use_container_width=True, hide_index=True)

    with col_r:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#FF3B30;margin-bottom:6px;'>📉 DECAYENDO</div>",
            unsafe_allow_html=True,
        )
        dc = comp[comp["Δ %"] < 0].tail(10).sort_values("Δ %").copy()
        dc["Δ %"] = dc["Δ %"].apply(lambda x: f"{x:.0f}%")
        st.dataframe(dc[["Producto", "Últimos 30d", "Δ %"]], use_container_width=True, hide_index=True)

    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin:16px 0 6px 0;'>EVOLUCIÓN SEMANAL</div>",
        unsafe_allow_html=True,
    )
    prods_disp = sorted(df_t["producto"].unique().tolist())
    sel_prods  = st.multiselect("Seleccionar productos", prods_disp, default=prods_disp[:3])

    if sel_prods:
        df_sel = df_t[df_t["producto"].isin(sel_prods)].copy()
        df_sel["semana"] = df_sel["fecha"].dt.to_period("W").dt.start_time
        evol   = df_sel.groupby(["semana", "producto"])["cantidad"].sum().reset_index()
        colores = ["#2D6A4F", "#FF3B30", "#FFB800", "#4488FF", "#FF6B35"]
        fig_ev  = go.Figure()
        for i, prod in enumerate(sel_prods):
            sub_p = evol[evol["producto"] == prod]
            fig_ev.add_trace(go.Scatter(
                x=sub_p["semana"], y=sub_p["cantidad"],
                mode="lines+markers", name=prod,
                line=dict(color=colores[i % len(colores)], width=2),
                marker=dict(size=5),
                hovertemplate="<b>%{fullData.name}</b><br>%{x|%d/%m}<br>%{y} u<extra></extra>",
            ))
        fig_ev.update_layout(
            **PLOT_BASE, height=300,
            margin=dict(t=10, b=30, l=50, r=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.01, x=0, font=dict(size=10)),
            xaxis=dict(showgrid=False, tickformat="%d/%m"),
            yaxis=dict(showgrid=True, gridcolor="#D4CFC4", tickfont=dict(size=9)),
        )
        st.plotly_chart(fig_ev, use_container_width=True, config={"displayModeBar": False})


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    check_google_login()
    token = shopify_get_token()

    with st.spinner("Cargando inventario desde Shopify..."):
        try:
            locations  = cargar_locations(token)
            productos  = cargar_productos(token)
            stock_map  = cargar_stock(token, productos)
            ventas_map = cargar_ventas_60d(token)
            df         = construir_df(productos, stock_map, ventas_map, locations)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                st.error("Token de Shopify inválido. Reconectando...")
                st.session_state.pop("shopify_token", None)
                st.rerun()
            st.error(f"Error de Shopify: {e}")
            st.stop()
        except Exception as e:
            st.error(f"Error cargando datos: {e}")
            st.stop()

    conteos = {}
    if not df.empty:
        for estado in ESTADOS:
            conteos[estado] = int(df[df["_estado"] == estado]["Producto"].nunique())

    if "vista" not in st.session_state:
        st.session_state.vista = "DASHBOARD"

    render_sidebar(conteos)

    vista = st.session_state.get("vista", "DASHBOARD")

    if vista == "DASHBOARD":
        vista_dashboard(df, locations)
    elif vista == "VENTAS":
        vista_ventas(token)
    elif vista == "ROTACION":
        vista_rotacion(df)
    elif vista == "TENDENCIAS":
        vista_tendencias(token)
    elif vista in ESTADOS:
        vista_inventario(df, vista, locations)

    st.markdown(
        f"<div style='font-size:10px;color:#D4CFC4;text-align:right;margin-top:40px;'>"
        f"LÍNEA VIVA v7 · TÉRRET · {datetime.now().strftime('%d.%m.%Y %H:%M')}</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
