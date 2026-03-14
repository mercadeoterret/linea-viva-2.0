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
LOCATIONS_EXCLUIR = ["Recogida en tienda (NO USAR)"]
LOCATIONS_VALIDAS = ["TERRET", "Tienda Fisica", "Tienda Móvil - Ferias"]
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

/* Radio buttons y labels */
[data-testid="stRadio"] label { color: #1A1A14 !important; }
[data-testid="stRadio"] p { color: #1A1A14 !important; }
[data-testid="stRadio"] span { color: #1A1A14 !important; }
div[role="radiogroup"] label { color: #1A1A14 !important; }
div[role="radiogroup"] p { color: #1A1A14 !important; }

/* Selectbox y otros labels */
[data-testid="stSelectbox"] label { color: #1A1A14 !important; }
[data-testid="stSelectbox"] p { color: #1A1A14 !important; }
label[data-testid="stWidgetLabel"] { color: #1A1A14 !important; }
label[data-testid="stWidgetLabel"] p { color: #1A1A14 !important; }

/* Toggle */
[data-testid="stToggle"] label { color: #1A1A14 !important; }
[data-testid="stToggle"] p { color: #1A1A14 !important; }

/* Todos los párrafos en el body principal */
[data-testid="stAppViewContainer"] p { color: #1A1A14 !important; }
[data-testid="stAppViewContainer"] span { color: #1A1A14 !important; }

/* Radio button circle colors */
[data-baseweb="radio"] div { border-color: #2D6A4F !important; }
[data-baseweb="radio"] [aria-checked="true"] div { background: #2D6A4F !important; }
[data-testid="stDataFrame"] { background: #EDEAE0 !important; }
[data-testid="stDataFrame"] iframe { background: #EDEAE0 !important; }
.stDataFrame { background: #EDEAE0 !important; }
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
    if st.session_state.get("logged_in"):
        return

    # Si viene callback de Shopify, dejar pasar sin pedir login
    if st.query_params.get("state", "") == "lv7":
        return

    client_id     = st.secrets.get("GOOGLE_CLIENT_ID", "")
    client_secret = st.secrets.get("GOOGLE_CLIENT_SECRET", "")
    redirect_uri  = st.secrets.get("REDIRECT_URI", "https://linea-viva-20-bdm63phvwl6idtuxaqvkqp.streamlit.app/")

    query_params = st.query_params
    auth_code = query_params.get("code")

    if not auth_code:
        auth_url = "https://accounts.google.com/o/oauth2/v2/auth"
        params = {
            "client_id":     client_id,
            "redirect_uri":  redirect_uri,
            "response_type": "code",
            "scope":         "openid email profile",
            "access_type":   "offline",
            "prompt":        "select_account",
        }
        login_url = f"{auth_url}?{urllib.parse.urlencode(params)}"

        st.markdown(
            "<div style='max-width:380px;margin:80px auto;text-align:center;'>"
            "<div style='background:#2D6A4F;width:56px;height:56px;border-radius:10px;"
            "display:inline-flex;align-items:center;justify-content:center;"
            "font-family:Bebas Neue,sans-serif;font-size:26px;color:#F5F0E8;"
            "margin-bottom:20px;'>LV</div>"
            "<div style='font-family:Bebas Neue,sans-serif;font-size:32px;letter-spacing:3px;"
            "color:#1A1A14;margin-bottom:4px;'>LÍNEA VIVA</div>"
            "<div style='font-size:10px;color:#6B6456;letter-spacing:2px;"
            "text-transform:uppercase;margin-bottom:8px;'>Térret · Inventario</div>"
            "<div style='font-size:12px;color:#B8B0A4;margin-bottom:32px;'>"
            "Acceso restringido · Solo @terretsports.com y @terret.co</div>"
            "</div>",
            unsafe_allow_html=True,
        )
        _, col, _ = st.columns([1, 2, 1])
        with col:
            st.link_button("🔑 INICIAR SESIÓN CON GOOGLE", login_url, use_container_width=True)
        st.stop()

    else:
        token_url = "https://oauth2.googleapis.com/token"
        data = {
            "code":          auth_code,
            "client_id":     client_id,
            "client_secret": client_secret,
            "redirect_uri":  redirect_uri,
            "grant_type":    "authorization_code",
        }
        response = requests.post(token_url, data=data)

        if response.status_code == 200:
            tokens       = response.json()
            access_token = tokens.get("access_token")

            user_info = requests.get(
                "https://www.googleapis.com/oauth2/v2/userinfo",
                headers={"Authorization": f"Bearer {access_token}"},
            ).json()

            user_email  = user_info.get("email", "").lower()
            user_domain = user_email.split("@")[-1] if "@" in user_email else ""

            allowed_domains = [d.strip().lower() for d in
                               st.secrets.get("ALLOWED_DOMAINS", "terretsports.com,terret.co").split(",")
                               if d.strip()]

            if user_domain not in allowed_domains:
                st.error(f"Acceso denegado: {user_email}. Solo cuentas @terretsports.com y @terret.co.")
                st.query_params.clear()
                if st.button("Probar con otra cuenta"):
                    st.rerun()
                st.stop()

            st.session_state.logged_in  = True
            st.session_state.user_email = user_email
            st.session_state.user_name  = user_info.get("name", "")
            st.query_params.clear()
            st.rerun()

        else:
            st.error("La sesión ha expirado o hubo un error de conexión con Google.")
            st.query_params.clear()
            if st.button("Volver a intentar"):
                st.rerun()
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
      products(first: 250, after: $cursor, query: "status:active") {
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
            iid       = str(n["inventory_item_id"])
            lid       = str(n["location_id"])
            available = max(0, int(n.get("available", 0) or 0))
            on_hand   = max(0, int(n.get("on_hand",   0) or 0))
            if iid not in stock_map:
                stock_map[iid] = {}
            stock_map[iid][lid] = {"available": available, "on_hand": on_hand}
    return stock_map


@st.cache_data(ttl=3600)
def cargar_ventas_60d(_token, _locations):
    """
    Retorna:
      ventas_global: {variant_id: total_qty}
      ventas_por_loc: {variant_id: {loc_name: qty}}
    location_id en la orden = punto de venta donde se procesó (igual que Apps Script)
    """
    loc_id_to_name = {str(loc["id"]): loc["name"] for loc in _locations}
    ONLINE = "TERRET"  # ventas sin location_id = canal online/bodega principal

    desde = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    orders = rest_paginated(
        _token, "orders.json", "orders",
        {"status": "any", "created_at_min": desde,
         "fields": "id,location_id,line_items", "limit": 250},
    )
    ventas_global  = {}
    ventas_por_loc = {}

    for order in orders:
        loc_id   = str(order.get("location_id") or "")
        loc_name = loc_id_to_name.get(loc_id, ONLINE)

        for item in order.get("line_items", []):
            vid = str(item.get("variant_id", ""))
            if not vid or vid == "None":
                continue
            qty = int(item.get("quantity", 0))
            ventas_global[vid] = ventas_global.get(vid, 0) + qty
            if vid not in ventas_por_loc:
                ventas_por_loc[vid] = {}
            ventas_por_loc[vid][loc_name] = ventas_por_loc[vid].get(loc_name, 0) + qty

    return ventas_global, ventas_por_loc


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
    """ventas_map = (ventas_global, ventas_por_loc)"""
    ventas_global, ventas_por_loc = ventas_map
    loc_id_to_name = {str(loc["id"]): loc["name"] for loc in locations}
    rows = []
    for prod in productos:
        for var in prod["variants"]:
            iid        = var["inventory_item_id"]
            vid        = var["variant_id"]
            loc_stocks = stock_map.get(iid, {})

            stock_total   = sum(v["available"] for v in loc_stocks.values())
            on_hand_total = sum(v["on_hand"]   for v in loc_stocks.values())
            committed     = max(0, on_hand_total - stock_total)

            ventas60d    = ventas_global.get(vid, 0)
            v_por_loc    = ventas_por_loc.get(vid, {})
            dias_inv     = round(stock_total / (ventas60d / 60), 1) if ventas60d > 0 else 9999

            row = {
                "Producto":     prod["title"],
                "Tipo":         prod["product_type"],
                "Variante":     var["variant_title"],
                "SKU":          var["sku"],
                "Precio Venta": var["price"],
                "Costo":        var["cost"],
                "Stock":        stock_total,
                "StockFisico":  on_hand_total,
                "Comprometido": committed,
                "Ventas60d":    ventas60d,
                "DiasInv_n":    dias_inv,
                "_variant_id":  vid,
                "_inv_item_id": iid,
                "_product_id":  prod["product_id"],
            }
            # Stock por location
            for loc_id, loc_name in loc_id_to_name.items():
                loc_data = loc_stocks.get(loc_id, {"available": 0, "on_hand": 0})
                row[f"Stock_{loc_name}"]   = loc_data["available"]
                row[f"Fisico_{loc_name}"]  = loc_data["on_hand"]
            # Ventas por location
            for loc_name in loc_id_to_name.values():
                row[f"Ventas_{loc_name}"] = v_por_loc.get(loc_name, 0)
            # Ventas online (sin location_id)
            row["Ventas_TERRET"] = v_por_loc.get("TERRET", 0)
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

def _seccion(titulo, subtitulo=""):
    sub_html = f"<div style='font-size:11px;color:#6B6456;letter-spacing:0.5px;margin-top:2px;'>{subtitulo}</div>" if subtitulo else ""
    st.markdown(
        f"<div style='margin:32px 0 16px 0;padding-bottom:10px;border-bottom:2px solid #D4CFC4;'>"
        f"<div style='font-family:Bebas Neue,sans-serif;font-size:18px;letter-spacing:3px;color:#1A1A14;'>{titulo}</div>"
        f"{sub_html}</div>",
        unsafe_allow_html=True,
    )


def vista_dashboard(df, locations):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:4px;'>DASHBOARD</div>"
        f"<div style='font-size:11px;color:#6B6456;letter-spacing:1px;"
        f"text-transform:uppercase;margin-bottom:20px;'>"
        f"Vision general del inventario · {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>",
        unsafe_allow_html=True,
    )

    if df.empty:
        st.warning("Sin datos. Verifica la conexion con Shopify.")
        return

    # ── Filtro de location ────────────────────────────────────────────────────
    loc_names = [loc["name"] for loc in locations]
    loc_cols  = [c for c in df.columns if c.startswith("Stock_")]
    df_view   = df.copy()
    sel_loc   = "Todas las sucursales"  # default

    if loc_cols:
        loc_names_filtrados = [n for n in loc_names if n not in LOCATIONS_EXCLUIR]
        fc1, fc2 = st.columns([2, 1])
        with fc1:
            sel_loc = st.selectbox("📍 Filtrar por sucursal:", ["Todas las sucursales"] + loc_names_filtrados, key="dash_loc")
        with fc2:
            tipo_inv = st.radio("📦 Tipo:", ["Disponible", "Físico"], horizontal=True, key="dash_tipo_inv")

        usar_fisico = tipo_inv == "Físico"

        if sel_loc != "Todas las sucursales":
            col_disp  = f"Stock_{sel_loc}"
            col_fisico = f"Fisico_{sel_loc}"
            col_usar  = col_fisico if usar_fisico else col_disp
            if col_usar in df_view.columns:
                df_view["Stock"]        = df_view[col_usar].clip(lower=0)
                df_view["DiasInv_n"]    = df_view.apply(
                    lambda r: round(r["Stock"] / (r["Ventas60d"] / 60), 1) if r["Ventas60d"] > 0 else 9999, axis=1)
                df_view["_estado"]      = df_view.apply(lambda r: calcular_estado(r["Stock"], r["Ventas60d"], r["DiasInv_n"]), axis=1)
                df_view["_valor_costo"] = df_view["Stock"] * df_view["Costo"]
                df_view["_valor_venta"] = df_view["Stock"] * df_view["Precio Venta"]
        else:
            # Todas las sucursales: toggle disponible vs físico global
            if usar_fisico:
                df_view["Stock"]        = df_view["StockFisico"]
                df_view["DiasInv_n"]    = df_view.apply(
                    lambda r: round(r["Stock"] / (r["Ventas60d"] / 60), 1) if r["Ventas60d"] > 0 else 9999, axis=1)
                df_view["_estado"]      = df_view.apply(lambda r: calcular_estado(r["Stock"], r["Ventas60d"], r["DiasInv_n"]), axis=1)
                df_view["_valor_costo"] = df_view["Stock"] * df_view["Costo"]
                df_view["_valor_venta"] = df_view["Stock"] * df_view["Precio Venta"]

        st.markdown("<hr style='border-color:#D4CFC4;margin:10px 0 20px 0;'>", unsafe_allow_html=True)

    tiene_costos  = df_view["Costo"].sum() > 0
    tiene_precios = df_view["Precio Venta"].sum() > 0

    # ── Metricas ──────────────────────────────────────────────────────────────
    # Filtrar por sucursal para SKUs/Productos: solo los que tienen stock > 0 en la sucursal
    df_con_stock = df_view[df_view["Stock"] > 0] if sel_loc != "Todas las sucursales" else df_view

    total_skus  = len(df_con_stock)
    total_prods = df_con_stock["Producto"].nunique()
    total_stock = int(df_view["Stock"].sum())
    reprog_n    = int(df_view[df_view["_estado"] == "REPROGRAMAR"]["Producto"].nunique())
    vc          = df_view["_valor_costo"].sum()
    vv          = df_view["_valor_venta"].sum()

    total_fisico    = int(df_view["StockFisico"].sum())
    total_comprometido = int(df_view["Comprometido"].sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("SKUs",             total_skus)
    with c2: st.metric("Productos",        total_prods)
    with c3: st.metric("Disponible",       f"{total_stock:,}")
    with c4: st.metric("Comprometido",     f"{total_comprometido:,}",
                        help="Unidades reservadas en órdenes pendientes")
    with c5: st.metric("A reprogramar",    reprog_n)

    if tiene_costos or tiene_precios:
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Valor inventario (costo)", "$" + f"{vc:,.0f}" if vc > 0 else "—")
        with c2: st.metric("Valor inventario (venta)", "$" + f"{vv:,.0f}" if vv > 0 else "—")
        with c3:
            mg = ((vv - vc) / vc * 100) if vc > 0 else 0
            st.metric("Margen potencial", f"{mg:.1f}%" if mg > 0 else "—")

    # ── Desglose por sucursal (solo en vista "Todas") ─────────────────────────
    if sel_loc == "Todas las sucursales" and loc_cols:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:12px;"
            "letter-spacing:2px;color:#6B6456;margin:16px 0 8px 0;'>"
            "STOCK DISPONIBLE POR SUCURSAL</div>",
            unsafe_allow_html=True,
        )
        locs_mostrar = [n for n in LOCATIONS_VALIDAS if f"Stock_{n}" in df_view.columns]
        cols_locs = st.columns(len(locs_mostrar))
        for i, loc_name in enumerate(locs_mostrar):
            col_disp   = f"Stock_{loc_name}"
            col_fisico = f"Fisico_{loc_name}"
            stock_loc  = int(df_view[col_disp].sum()) if col_disp in df_view.columns else 0
            fisico_loc = int(df_view[col_fisico].sum()) if col_fisico in df_view.columns else 0
            comp_loc   = max(0, fisico_loc - stock_loc)
            skus_loc   = int((df_view[col_disp] > 0).sum()) if col_disp in df_view.columns else 0
            with cols_locs[i]:
                st.markdown(
                    f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-radius:6px;"
                    f"padding:10px 14px;'>"
                    f"<div style='font-size:9px;letter-spacing:1.5px;text-transform:uppercase;"
                    f"color:#6B6456;margin-bottom:6px;'>{loc_name}</div>"
                    f"<div style='font-family:Bebas Neue,sans-serif;font-size:1.6rem;color:#2D6A4F;"
                    f"line-height:1;'>{stock_loc:,}</div>"
                    f"<div style='font-size:10px;color:#6B6456;margin-top:4px;'>"
                    f"{skus_loc} SKUs · {comp_loc:,} comprometidas</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    _seccion('VISIÓN GENERAL', 'Segmentos de inventario y productos críticos')

    # ── FILA 1: Pastel + Stock Critico ────────────────────────────────────────
    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown("<div style='font-family:Bebas Neue,sans-serif;font-size:13px;letter-spacing:2px;color:#6B6456;margin-bottom:8px;'>SEGMENTOS</div>", unsafe_allow_html=True)
        seg = df_view.groupby("_estado")["Producto"].nunique().reset_index()
        seg.columns = ["Estado", "Productos"]
        seg = seg[seg["Productos"] > 0]

        colores_pie = [color_estado(e) for e in seg["Estado"]]
        labels_pie  = [ESTADOS.get(e, {}).get("label", e) for e in seg["Estado"]]

        fig_pie = go.Figure(go.Pie(
            labels=labels_pie,
            values=seg["Productos"],
            hole=0.55,
            marker=dict(colors=colores_pie, line=dict(color="#F5F0E8", width=2)),
            textinfo="label+percent",
            textfont=dict(size=12, color="#1A1A14"),
            hovertemplate="<b>%{label}</b><br>%{value} productos<br>%{percent}<extra></extra>",
        ))
        fig_pie.update_layout(
            paper_bgcolor="#EDEAE0",
            plot_bgcolor="#EDEAE0",
            font=dict(color="#1A1A14", family="DM Sans"),
            margin=dict(t=30, b=30, l=10, r=10),
            height=380,
            showlegend=False,
            annotations=[dict(
                text="<b>" + str(total_prods) + "</b><br>productos",
                x=0.5, y=0.5, font_size=18, showarrow=False,
                font=dict(color="#1A1A14"),
            )],
        )
        st.plotly_chart(fig_pie, use_container_width=True, config={"displayModeBar": False})

    with col_r:
        st.markdown("<div style='font-family:Bebas Neue,sans-serif;font-size:13px;letter-spacing:2px;color:#6B6456;margin-bottom:8px;'>STOCK CRÍTICO — TOP 10</div>", unsafe_allow_html=True)
        criticos = (
            df_view[df_view["_estado"] == "REPROGRAMAR"]
            .groupby("Producto")
            .agg(ventas=("Ventas60d", "sum"), stock=("Stock", "sum"), dias_min=("DiasInv_n", "min"))
            .reset_index()
            .sort_values("ventas", ascending=False)
            .head(10)
            .sort_values("ventas", ascending=True)
        )
        if criticos.empty:
            st.markdown(
                "<div style='text-align:center;padding:40px;color:#6B6456;'>Sin productos criticos</div>",
                unsafe_allow_html=True,
            )
        else:
            def label_crit(row):
                return "QUIEBRE" if row["stock"] == 0 else f"{int(row['dias_min'])}d"

            criticos["_label"] = criticos["Producto"].apply(
                lambda x: x[:32] + "..." if len(x) > 32 else x)
            fig_crit = go.Figure(go.Bar(
                x=criticos["ventas"],
                y=criticos["_label"],
                orientation="h",
                marker=dict(
                    color=criticos["stock"].apply(lambda s: "#FF3B30" if s == 0 else "#FFB800"),
                    opacity=0.85,
                ),
                text=criticos.apply(label_crit, axis=1),
                textposition="outside",
                textfont=dict(size=11, color="#1A1A14"),
                hovertemplate="<b>%{y}</b><br>%{x} u vendidas 60d<extra></extra>",
            ))
            fig_crit.update_layout(
                paper_bgcolor="#EDEAE0",
                plot_bgcolor="#EDEAE0",
                font=dict(color="#1A1A14", family="DM Sans"),
                margin=dict(t=10, b=10, l=260, r=100),
                height=380,
                xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False, showticklabels=False,
                           range=[0, criticos["ventas"].max() * 1.4]),
                yaxis=dict(showgrid=False, tickfont=dict(size=11, color="#1A1A14"), tickcolor="#1A1A14"),
            )
            st.plotly_chart(fig_crit, use_container_width=True, config={"displayModeBar": False})

    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    _label_loc = f' · {sel_loc}' if sel_loc != 'Todas las sucursales' else ''
    _seccion('TOP VENTAS 60D', f'Últimos 60 días{_label_loc}')
    # ── FILA 2: Top Ventas (ancho completo) ──────────────────────────────────
    tc1, tc2, tc3 = st.columns([8, 1, 1])
    with tc1:
        pass
    with tc2:
        n_top = st.select_slider("", options=[10, 15, 20, 30, 50], value=10,
                                 key="slider_top_ventas", label_visibility="collapsed")
    with tc3:
        vista_sku = st.toggle("Por SKU", key="toggle_top_sku", value=False)


    if True:  # bloque unico para mantener indentacion

        if vista_sku:
            top_data = df_view[["Producto","Variante","SKU","Ventas60d","_estado"]].copy()
            top_data = top_data.sort_values("Ventas60d", ascending=True).tail(n_top)
            top_data["etiqueta"] = top_data["SKU"] + "  " + top_data["Variante"].str[:18]
            y_vals  = top_data["etiqueta"].tolist()
            x_vals  = top_data["Ventas60d"].tolist()
            estados = top_data["_estado"].tolist()
        else:
            # Si hay sucursal seleccionada, usar ventas de esa sucursal
            col_ventas = f"Ventas_{sel_loc}" if sel_loc != "Todas las sucursales" and f"Ventas_{sel_loc}" in df_view.columns else "Ventas60d"

            top_data = (
                df_view.groupby("_product_id")
                .apply(lambda g: pd.Series({
                    "Producto":  g["Producto"].iloc[0],
                    "Ventas60d": g[col_ventas].sum(),
                    "_estado":   g.loc[g[col_ventas].idxmax(), "_estado"] if g[col_ventas].sum() > 0 else g["_estado"].iloc[0],
                }))
                .reset_index(drop=True)
            )
            top_data = top_data[top_data["Ventas60d"] > 0].sort_values("Ventas60d", ascending=True).tail(n_top)
            y_vals  = top_data["Producto"].tolist()
            x_vals  = top_data["Ventas60d"].tolist()
            estados = top_data["_estado"].tolist()

        colores_top = [
            "#2D6A4F" if e == "ESTRELLA" else
            "#FFB800" if e == "ALTA_ROTACION" else
            "#FF3B30" if e == "REPROGRAMAR" else "#4488FF"
            for e in estados
        ]
        max_x = max(x_vals) if x_vals else 1
        filas = list(zip(y_vals, x_vals, colores_top))[::-1]
        filas_html = "".join(
            f"<tr>"
            f"<td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;"
            f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{nombre}</td>"
            f"<td style='padding:7px 8px;width:35%;'>"
            f"<div style='background:#D4CFC4;border-radius:3px;height:14px;'>"
            f"<div style='background:{color};width:{int(val/max_x*100)}%;height:14px;"
            f"border-radius:3px;opacity:0.9;'></div></div></td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#6B6456;text-align:right;white-space:nowrap;'>{int(val)} u</td>"
            f"</tr>"
            for nombre, val, color in filas
        )
        st.markdown(
            f"<table style='width:100%;border-collapse:collapse;'>"
            f"<tbody>{filas_html}</tbody></table>",
            unsafe_allow_html=True,
        )

    _seccion('STOCK POR CATEGORÍA', 'Valor en inventario por línea de producto')
    por_tipo = (
        df_view[df_view["Tipo"].str.strip() != ""]
        .groupby("Tipo")
        .agg(stock=("Stock","sum"), valor_costo=("_valor_costo","sum"), valor_venta=("_valor_venta","sum"))
        .reset_index()
        .sort_values("stock", ascending=True)
    )
    por_tipo = por_tipo[por_tipo["stock"] > 0]
    x_cat   = por_tipo["valor_costo"] if tiene_costos else por_tipo["stock"]
    txt_cat = ["$" + f"{v:,.0f}" for v in x_cat] if tiene_costos else [str(int(v)) + " u" for v in x_cat]
    max_cat = x_cat.max() if len(x_cat) > 0 else 1
    cat_filas = list(zip(por_tipo["Tipo"].tolist()[::-1], x_cat.tolist()[::-1], txt_cat[::-1]))
    cat_html = "".join(
        f"<tr>"
        f"<td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;"
        f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{str(tipo)}</td>"
        f"<td style='padding:7px 8px;width:50%;'>"
        f"<div style='background:#D4CFC4;border-radius:3px;height:14px;'>"
        f"<div style='background:#4488FF;width:{int(val/max_cat*100)}%;height:14px;"
        f"border-radius:3px;opacity:0.85;'></div></div></td>"
        f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
        f"color:#6B6456;text-align:right;white-space:nowrap;'>{txt}</td>"
        f"</tr>"
        for tipo, val, txt in cat_filas
    )
    st.markdown(
        f"<table style='width:100%;border-collapse:collapse;'>"
        f"<tbody>{cat_html}</tbody></table>",
        unsafe_allow_html=True,
    )

    # ── FILA 3: Valor de Inventario por Categoria ─────────────────────────────
    if tiene_costos or tiene_precios:
        _seccion('VALOR DE INVENTARIO', 'Costo vs precio de venta · por categoría')
        pv = (
            df_view[df_view["Tipo"].str.strip() != ""]
            .groupby("Tipo")
            .agg(vc=("_valor_costo","sum"), vv=("_valor_venta","sum"))
            .reset_index()
            .sort_values("vv", ascending=True)
        )
        pv = pv[(pv["vc"] > 0) | (pv["vv"] > 0)]
        cats   = pv["Tipo"].tolist()
        costos = pv["vc"].tolist()
        ventas = pv["vv"].tolist()

        max_vv = max(ventas) if ventas else 1
        # Leyenda
        st.markdown(
            "<div style='display:flex;gap:20px;margin-bottom:8px;font-size:12px;'>"
            "<span style='display:flex;align-items:center;gap:6px;'>"
            "<span style='display:inline-block;width:14px;height:14px;background:#2D6A4F;border-radius:2px;opacity:0.85;'></span>"
            "<span style='color:#1A1A14;'>Precio venta</span></span>"
            "<span style='display:flex;align-items:center;gap:6px;'>"
            "<span style='display:inline-block;width:14px;height:14px;background:#4488FF;border-radius:2px;opacity:0.9;'></span>"
            "<span style='color:#1A1A14;'>Costo</span></span>"
            "</div>",
            unsafe_allow_html=True,
        )
        val_filas = list(zip(cats[::-1], costos[::-1], ventas[::-1]))
        val_html = "".join(
            f"<tr>"
            f"<td style='padding:6px 12px 6px 0;font-size:13px;font-weight:500;"
            f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;"
            f"min-width:140px;'>{cat}</td>"
            f"<td style='padding:6px 8px;width:55%;position:relative;'>"
            f"<div style='background:#D4CFC4;border-radius:3px;height:18px;position:relative;'>"
            f"<div style='background:#2D6A4F;width:{int(vv/max_vv*100)}%;height:18px;border-radius:3px;opacity:0.85;position:absolute;top:0;left:0;'></div>"
            f"<div style='background:#4488FF;width:{int(vc/max_vv*100)}%;height:18px;border-radius:3px;opacity:0.9;position:absolute;top:0;left:0;'></div>"
            f"</div></td>"
            f"<td style='padding:6px 4px;font-family:DM Mono,monospace;font-size:11px;"
            f"color:#2D6A4F;text-align:right;white-space:nowrap;'>{'$'+f'{vv/1e6:.1f}M' if vv>=1e6 else '$'+f'{vv:,.0f}'}</td>"
            f"<td style='padding:6px 0 6px 8px;font-family:DM Mono,monospace;font-size:11px;"
            f"color:#4488FF;text-align:right;white-space:nowrap;'>{'$'+f'{vc/1e6:.1f}M' if vc>=1e6 else '$'+f'{vc:,.0f}'}</td>"
            f"</tr>"
            for cat, vc, vv in val_filas
        )
        st.markdown(
            f"<table style='width:100%;border-collapse:collapse;'><tbody>{val_html}</tbody></table>",
            unsafe_allow_html=True,
        )

    _seccion('RESUMEN POR SEGMENTO')
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
            row["Valor costo"] = "$" + f"{sub['_valor_costo'].sum():,.0f}"
        if tiene_precios:
            row["Valor venta"] = "$" + f"{sub['_valor_venta'].sum():,.0f}"
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

    # ── Top Productos — tabla HTML ancho completo ────────────────────────────
    _seccion("TOP PRODUCTOS", f"Por valor de venta · {sel_rango}")
    tp = (
        df_v.groupby("producto")
        .agg(total=("total", "sum"), unidades=("cantidad", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
        .head(15)
    )
    max_tp = tp["total"].max() if len(tp) else 1
    tp_html = "".join(
        f"<tr>"
        f"<td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;"
        f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{row.producto}</td>"
        f"<td style='padding:7px 8px;width:45%;'>"
        f"<div style='background:#D4CFC4;border-radius:3px;height:16px;'>"
        f"<div style='background:#2D6A4F;width:{int(row.total/max_tp*100)}%;height:16px;"
        f"border-radius:3px;opacity:0.85;'></div></div></td>"
        f"<td style='padding:7px 4px;font-family:DM Mono,monospace;font-size:12px;"
        f"color:#2D6A4F;text-align:right;white-space:nowrap;font-weight:600;'>{fmt_pesos(row.total)}</td>"
        f"<td style='padding:7px 0 7px 12px;font-family:DM Mono,monospace;font-size:12px;"
        f"color:#6B6456;text-align:right;white-space:nowrap;'>{int(row.unidades)} u</td>"
        f"</tr>"
        for row in tp.itertuples()
    )
    st.markdown(
        f"<table style='width:100%;border-collapse:collapse;'><tbody>{tp_html}</tbody></table>",
        unsafe_allow_html=True,
    )

    # ── Detalle por SKU — cards agrupados por producto ────────────────────────
    _seccion("DETALLE POR SKU", "Variantes ordenadas por unidades vendidas")
    det = (
        df_v.groupby(["producto", "sku", "variante"])
        .agg(unidades=("cantidad", "sum"), total=("total", "sum"))
        .reset_index()
        .sort_values(["producto", "unidades"], ascending=[True, False])
    )
    for prod, grupo in det.groupby("producto", sort=False):
        total_prod = grupo["total"].sum()
        unids_prod = int(grupo["unidades"].sum())
        # Cabecera producto
        st.markdown(
            f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
            f"border-left:3px solid #2D6A4F;border-radius:6px 6px 0 0;"
            f"padding:10px 14px;display:flex;align-items:center;justify-content:space-between;'>"
            f"<div style='font-weight:600;font-size:13px;color:#1A1A14;"
            f"font-family:DM Sans,sans-serif;'>{prod}</div>"
            f"<div style='font-family:DM Mono,monospace;font-size:12px;color:#2D6A4F;'>"
            f"{fmt_pesos(total_prod)} · {unids_prod} u</div>"
            f"</div>",
            unsafe_allow_html=True,
        )
        # Filas de variantes
        rows_sku = "".join(
            f"<tr style='border-top:1px solid #D4CFC4;'>"
            f"<td style='padding:6px 14px;font-size:12px;color:#6B6456;"
            f"font-family:DM Mono,monospace;white-space:nowrap;'>{r.sku}</td>"
            f"<td style='padding:6px 14px;font-size:12px;color:#1A1A14;"
            f"font-family:DM Sans,sans-serif;'>{r.variante}</td>"
            f"<td style='padding:6px 14px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#1A1A14;text-align:right;white-space:nowrap;'>{int(r.unidades)} u</td>"
            f"<td style='padding:6px 14px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#2D6A4F;text-align:right;white-space:nowrap;font-weight:600;'>{fmt_pesos(r.total)}</td>"
            f"</tr>"
            for r in grupo.itertuples()
        )
        st.markdown(
            f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
            f"border-left:3px solid #2D6A4F;border-radius:0 0 6px 6px;overflow:hidden;'>"
            f"<table style='width:100%;border-collapse:collapse;'><tbody>{rows_sku}</tbody></table>"
            f"</div><div style='height:6px;'></div>",
            unsafe_allow_html=True,
        )


# ─── MÓDULO 3: ROTACIÓN ───────────────────────────────────────────────────────

def vista_rotacion(df):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:4px;'>ROTACIÓN DE INVENTARIO</div>"
        "<div style='font-size:11px;color:#6B6456;letter-spacing:1px;"
        "text-transform:uppercase;margin-bottom:20px;'>"
        "Convierte capital inmovilizado en stock de lo que sí vende</div>",
        unsafe_allow_html=True,
    )
    if df.empty:
        st.warning("Sin datos.")
        return

    liq = df[df["_estado"] == "LIQUIDAR"].copy()
    rep = df[df["_estado"].isin(["REPROGRAMAR", "ESTRELLA", "ALTA_ROTACION"])].copy()

    # ── PASO 1: Capital disponible en liquidación ──────────────────────────────
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:14px;"
        "letter-spacing:2px;color:#FF9500;margin-bottom:8px;'>PASO 1 — CAPITAL INMOVILIZADO (LIQUIDAR)</div>",
        unsafe_allow_html=True,
    )

    desc_pct = st.slider("Descuento de liquidación (%)", 10, 60, 30, 5, key="desc_liq")
    factor   = 1 - desc_pct / 100

    capital_total = 0.0
    liq_ag = pd.DataFrame()

    if liq.empty:
        st.info("No hay productos en LIQUIDAR actualmente.")
    else:
        liq_ag = liq.groupby("Producto").agg(
            stock=("Stock", "sum"),
            precio=("Precio Venta", "mean"),
            costo=("Costo", "mean"),
            ventas=("Ventas60d", "sum"),
        ).reset_index()
        liq_ag = liq_ag[liq_ag["stock"] > 0].copy()
        liq_ag["precio_liq"]        = liq_ag["precio"] * factor
        liq_ag["valor_costo"]       = liq_ag["stock"] * liq_ag["costo"]
        liq_ag["capital_liq"]       = liq_ag["stock"] * liq_ag["precio_liq"]
        capital_total = liq_ag["capital_liq"].sum()

        liq_plot = liq_ag.sort_values("capital_liq", ascending=False).head(15)
        max_liq = liq_plot["capital_liq"].max() or 1
        liq_html = "".join(
            f"<tr>"
            f"<td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;"
            f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{r['Producto']}</td>"
            f"<td style='padding:7px 8px;width:40%;'>"
            f"<div style='background:#D4CFC4;border-radius:3px;height:16px;'>"
            f"<div style='background:#FF9500;width:{min(100,int(r['capital_liq']/max_liq*100))}%;height:16px;"
            f"border-radius:3px;opacity:0.85;'></div></div></td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#FF9500;text-align:right;white-space:nowrap;font-weight:600;'>{fmt_pesos(r['capital_liq'])}</td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#6B6456;text-align:right;white-space:nowrap;'>{int(r['stock'])} u</td>"
            f"</tr>"
            for _, r in liq_plot.iterrows()
        )
        st.markdown(
            f"<table style='width:100%;border-collapse:collapse;'><tbody>{liq_html}</tbody></table>",
            unsafe_allow_html=True,
        )

        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Productos a liquidar", len(liq_ag))
        with c2: st.metric("Unidades totales",     int(liq_ag["stock"].sum()))
        with c3: st.metric(f"Capital estimado ({desc_pct}% desc.)", fmt_pesos(capital_total))

    st.markdown("<hr style='border-color:#D4CFC4;margin:24px 0;'>", unsafe_allow_html=True)

    # ── PASO 2: Calculadora de reposición ─────────────────────────────────────
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:14px;"
        "letter-spacing:2px;color:#2D6A4F;margin-bottom:8px;'>PASO 2 — ¿QUÉ REPONGO CON ESE CAPITAL?</div>",
        unsafe_allow_html=True,
    )

    presupuesto = st.number_input(
        "Presupuesto disponible ($COP)",
        min_value=0, value=int(capital_total), step=100_000, key="presupuesto_rot",
        help="Puedes ajustar este valor. Por defecto es el capital estimado de liquidación.",
    )

    if rep.empty:
        st.info("No hay productos en REPROGRAMAR, ESTRELLA o ALTA_ROTACION.")
        return

    rep_ag = rep.groupby("Producto").agg(
        costo=("Costo", "mean"),
        ventas=("Ventas60d", "sum"),
        stock=("Stock", "sum"),
        dias=("DiasInv_n", "min"),
        estado=("_estado", "first"),
    ).reset_index()
    rep_ag = rep_ag[rep_ag["costo"] > 0].sort_values("ventas", ascending=False)

    rep_ag["sug_unids"] = rep_ag.apply(
        lambda r: sugerir_cantidad(r["stock"], r["ventas"], r["dias"], r["estado"])[0], axis=1
    )
    rep_ag["costo_sug"] = rep_ag["sug_unids"] * rep_ag["costo"]

    # Asignar presupuesto en orden de prioridad (mayor ventas primero)
    presupuesto_rest = float(presupuesto)
    rep_ag["unids_posibles"] = 0
    rep_ag["costo_real"]     = 0.0

    for idx, row in rep_ag.iterrows():
        if presupuesto_rest <= 0 or row["costo"] <= 0 or row["sug_unids"] == 0:
            continue
        max_u = int(presupuesto_rest / row["costo"])
        unids = min(max_u, row["sug_unids"])
        unids = (unids // MULTIPLO) * MULTIPLO
        if unids < MULTIPLO:
            continue
        rep_ag.at[idx, "unids_posibles"] = unids
        rep_ag.at[idx, "costo_real"]     = unids * row["costo"]
        presupuesto_rest -= unids * row["costo"]

    rep_con = rep_ag[rep_ag["unids_posibles"] > 0].copy()
    rep_sin = rep_ag[rep_ag["unids_posibles"] == 0].copy()

    if not rep_con.empty:
        rep_plot = rep_con.sort_values("costo_real", ascending=False)
        max_sug = rep_plot["costo_sug"].max() or 1
        # Leyenda
        st.markdown(
            "<div style='display:flex;gap:20px;margin-bottom:8px;font-size:12px;'>"
            "<span style='display:flex;align-items:center;gap:6px;'>"
            "<span style='display:inline-block;width:14px;height:14px;background:#2D6A4F;"
            "border-radius:2px;'></span><span style='color:#1A1A14;'>Posible reponer</span></span>"
            "<span style='display:flex;align-items:center;gap:6px;'>"
            "<span style='display:inline-block;width:14px;height:14px;background:#D4CFC4;"
            "border-radius:2px;'></span><span style='color:#1A1A14;'>Sugerido total</span></span>"
            "</div>",
            unsafe_allow_html=True,
        )
        rep_html = "".join(
            f"<tr>"
            f"<td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;"
            f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{r['Producto']}</td>"
            f"<td style='padding:7px 8px;width:40%;position:relative;'>"
            f"<div style='background:#D4CFC4;border-radius:3px;height:18px;position:relative;'>"
            f"<div style='background:#D4CFC4;width:{min(100,int(r['costo_sug']/max_sug*100))}%;height:18px;"
            f"border-radius:3px;position:absolute;top:0;left:0;opacity:0.5;'></div>"
            f"<div style='background:#2D6A4F;width:{min(100,int(r['costo_real']/max_sug*100))}%;height:18px;"
            f"border-radius:3px;position:absolute;top:0;left:0;opacity:0.9;'></div>"
            f"</div></td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#2D6A4F;text-align:right;white-space:nowrap;font-weight:600;'>"
            f"{int(r['unids_posibles'])} u</td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#1A1A14;text-align:right;white-space:nowrap;'>"
            f"{fmt_pesos(r['costo_real'])}</td>"
            f"</tr>"
            for _, r in rep_plot.iterrows()
        )
        st.markdown(
            f"<table style='width:100%;border-collapse:collapse;'><tbody>{rep_html}</tbody></table>",
            unsafe_allow_html=True,
        )

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Productos a reponer",  len(rep_con))
    with c2: st.metric("Unidades totales",     int(rep_ag["unids_posibles"].sum()))
    with c3: st.metric("Capital a invertir",   fmt_pesos(rep_ag["costo_real"].sum()))
    with c4: st.metric("Presupuesto restante", fmt_pesos(presupuesto_rest))

    if not rep_sin.empty:
        st.markdown(
            f"<div style='font-size:11px;color:#B8B0A4;margin-top:8px;'>"
            f"{len(rep_sin)} productos necesitan reposición pero el presupuesto no alcanza: "
            + ", ".join(rep_sin["Producto"].str[:25].tolist()[:5])
            + ("..." if len(rep_sin) > 5 else "") +
            "</div>",
            unsafe_allow_html=True,
        )


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
    hoy   = pd.Timestamp.now()
    corte = hoy - timedelta(days=30)
    inicio = hoy - timedelta(days=90)

    rec = df_t[df_t["fecha"] >= corte].groupby("producto")["cantidad"].sum()
    ant = df_t[(df_t["fecha"] >= inicio) & (df_t["fecha"] < corte)].groupby("producto")["cantidad"].sum()

    comp = pd.DataFrame({"reciente": rec, "anterior": ant}).fillna(0)

    # Solo productos que vendieron en AMBOS períodos — eliminar ruido
    comp = comp[(comp["reciente"] >= 3) & (comp["anterior"] >= 3)].copy()

    comp["delta"] = comp["reciente"] - comp["anterior"]
    comp["pct"]   = (comp["delta"] / comp["anterior"] * 100).round(0)
    comp = comp.reset_index()
    comp.columns = ["Producto", "Últimos 30d", "30d ant.", "Δ u", "Δ %"]
    comp = comp.sort_values("Δ %", ascending=False)

    # ── Gráfico principal: acelerando vs desacelerando ────────────────────────
    top_crec = comp[comp["Δ %"] > 0].head(10)
    top_dec  = comp[comp["Δ %"] < 0].tail(10).sort_values("Δ %")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#2D6A4F;margin-bottom:10px;'>📈 ACELERANDO</div>",
            unsafe_allow_html=True,
        )
        if top_crec.empty:
            st.info("Sin productos con tendencia creciente significativa.")
        else:
            max_crec = top_crec["Δ u"].quantile(0.85) or top_crec["Δ u"].max() or 1
            crec_html = "".join(
                f"<tr>"
                f"<td style='padding:7px 12px 7px 0;font-size:12px;font-weight:500;"
                f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{r['Producto']}</td>"
                f"<td style='padding:7px 8px;width:35%;'>"
                f"<div style='background:#D4CFC4;border-radius:3px;height:14px;'>"
                f"<div style='background:#2D6A4F;width:{min(100,int(r['Δ u']/max_crec*100))}%;height:14px;"
                f"border-radius:3px;opacity:0.85;'></div></div></td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#2D6A4F;text-align:right;white-space:nowrap;font-weight:600;'>"
                f"+{int(r['Últimos 30d'])} u</td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#6B6456;text-align:right;white-space:nowrap;'>"
                f"{r['Δ %']:+.0f}%</td>"
                f"</tr>"
                for _, r in top_crec.iterrows()
            )
            st.markdown(
                f"<table style='width:100%;border-collapse:collapse;'><tbody>{crec_html}</tbody></table>",
                unsafe_allow_html=True,
            )

    with col_r:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#FF3B30;margin-bottom:10px;'>📉 DESACELERANDO</div>",
            unsafe_allow_html=True,
        )
        if top_dec.empty:
            st.info("Sin productos con tendencia decreciente significativa.")
        else:
            max_dec = top_dec["Δ u"].abs().quantile(0.85) or top_dec["Δ u"].abs().max() or 1
            dec_html = "".join(
                f"<tr>"
                f"<td style='padding:7px 12px 7px 0;font-size:12px;font-weight:500;"
                f"color:#1A1A14;font-family:DM Sans,sans-serif;white-space:nowrap;'>{r['Producto']}</td>"
                f"<td style='padding:7px 8px;width:35%;'>"
                f"<div style='background:#D4CFC4;border-radius:3px;height:14px;'>"
                f"<div style='background:#FF3B30;width:{min(100,int(abs(r['Δ u'])/max_dec*100))}%;height:14px;"
                f"border-radius:3px;opacity:0.75;'></div></div></td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#FF3B30;text-align:right;white-space:nowrap;font-weight:600;'>"
                f"{int(r['Últimos 30d'])} u</td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#6B6456;text-align:right;white-space:nowrap;'>"
                f"{r['Δ %']:+.0f}%</td>"
                f"</tr>"
                for _, r in top_dec.iterrows()
            )
            st.markdown(
                f"<table style='width:100%;border-collapse:collapse;'><tbody>{dec_html}</tbody></table>",
                unsafe_allow_html=True,
            )

    st.markdown(
        "<div style='font-size:10px;color:#B8B0A4;margin-bottom:20px;'>"
        "Solo se muestran productos con ≥ 3 unidades vendidas en ambos períodos para eliminar ruido estadístico."
        "</div>",
        unsafe_allow_html=True,
    )

    # ── Evolución semanal ─────────────────────────────────────────────────────
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin:16px 0 6px 0;'>EVOLUCIÓN SEMANAL — DRILL DOWN</div>",
        unsafe_allow_html=True,
    )

    # Por defecto mostrar los top 5 más vendidos en los últimos 30d
    top5 = df_t[df_t["fecha"] >= corte].groupby("producto")["cantidad"].sum().nlargest(5).index.tolist()
    prods_disp = sorted(df_t["producto"].unique().tolist())
    sel_prods  = st.multiselect("Seleccionar productos", prods_disp, default=top5[:3], key="sel_tend")

    if sel_prods:
        df_sel = df_t[df_t["producto"].isin(sel_prods)].copy()
        df_sel["semana"] = df_sel["fecha"].dt.to_period("W").dt.start_time
        evol   = df_sel.groupby(["semana", "producto"])["cantidad"].sum().reset_index()
        colores = ["#2D6A4F", "#FF3B30", "#FFB800", "#4488FF", "#FF6B35"]

        fig_ev = go.Figure()
        for i, prod in enumerate(sel_prods):
            sub_p = evol[evol["producto"] == prod]
            if sub_p.empty:
                continue
            fig_ev.add_trace(go.Scatter(
                x=sub_p["semana"], y=sub_p["cantidad"],
                mode="lines+markers", name=prod,
                line=dict(color=colores[i % len(colores)], width=2),
                marker=dict(size=6),
                hovertemplate="<b>%{fullData.name}</b><br>Semana %{x|%d %b}<br>%{y} u<extra></extra>",
            ))

        # Línea vertical en el corte 30d
        fig_ev.add_vline(
            x=corte.timestamp() * 1000,
            line=dict(color="#B8B0A4", width=1, dash="dot"),
            annotation_text="hace 30d",
            annotation_font_size=9,
        )
        fig_ev.update_layout(
            **PLOT_BASE, height=320,
            margin=dict(t=20, b=30, l=50, r=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=10)),
            xaxis=dict(showgrid=False, tickformat="%d %b"),
            yaxis=dict(showgrid=True, gridcolor="#D4CFC4", tickfont=dict(size=9), title="Unidades"),
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
            ventas_map = cargar_ventas_60d(token, locations)
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
