"""
Línea Viva v9 — Inventario Inteligente para Térret
Shopify Admin API (REST + GraphQL) directo — sin Google Sheets para inventario.
OAuth Shopify integrado: si no hay token en secrets, la app misma hace el flujo.
v9: Sistema de clasificación multidimensional (Rotación + Stock + Acción)
"""

import math
import re
import random
import uuid
import urllib.parse
import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ─── PAGE CONFIG ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Línea Viva · Térret",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# ██████████████████████████████████████████████████████████████████████████████
#
#   SISTEMA DE CLASIFICACIÓN — LEER ANTES DE MODIFICAR CUALQUIER UMBRAL
#
# ══════════════════════════════════════════════════════════════════════════════
#
#   FILOSOFÍA: cada producto se evalúa en 3 dimensiones independientes.
#   Ninguna dimensión depende de la otra para calcularse.
#   La ACCIÓN es la conclusión que se deriva de cruzar las otras dos.
#
#   ┌─────────────────────────────────────────────────────────────────┐
#   │  DIMENSIÓN 1 — ROTACIÓN                                         │
#   │  Pregunta: ¿cuánto vende este producto?                         │
#   │  Fuente: unidades vendidas en los últimos 60 días               │
#   │  No depende del stock — solo del comportamiento de venta        │
#   │                                                                 │
#   │  ALTA      → ventas60d >= ROT_ALTA      (default: 10 u)        │
#   │  MEDIA     → ventas60d >= ROT_MEDIA     (default:  4 u)        │
#   │  BAJA      → ventas60d >= ROT_BAJA      (default:  1 u)        │
#   │  NULA      → ventas60d == 0                                     │
#   └─────────────────────────────────────────────────────────────────┘
#
#   ┌─────────────────────────────────────────────────────────────────┐
#   │  DIMENSIÓN 2 — STOCK                                            │
#   │  Pregunta: ¿cuántos días de inventario quedan?                  │
#   │  Fuente: stock_actual / (ventas60d / 60) = días de cobertura    │
#   │  Si ventas == 0 → cobertura = INF (no hay referencia de venta)  │
#   │                                                                 │
#   │  EXCESO    → dias_cobertura >= STOCK_EXCESO   (default: 120d)  │
#   │  SALUDABLE → dias_cobertura >= STOCK_SALUDABLE (default: 30d)  │
#   │  BAJO      → dias_cobertura >  0                                │
#   │  HUECO     → stock == 0                                         │
#   └─────────────────────────────────────────────────────────────────┘
#
#   ┌─────────────────────────────────────────────────────────────────┐
#   │  DIMENSIÓN 3 — ACCIÓN (derivada, no modificar directamente)     │
#   │  Pregunta: ¿qué hago con este producto?                         │
#   │  Fuente: cruce de Rotación × Stock (ver tabla más abajo)        │
#   │                                                                 │
#   │  Tabla de decisión:                                             │
#   │                                                                 │
#   │  Rotación  │  Stock    │  Acción                               │
#   │  ──────────┼───────────┼────────────────────────────────────── │
#   │  ALTA      │  HUECO    │  REPROGRAMAR ⚡ (quiebre total)       │
#   │  ALTA      │  BAJO     │  REPROGRAMAR ⚡ (urgente)             │
#   │  ALTA      │  SALUDABLE│  OK ✅                                │
#   │  ALTA      │  EXCESO   │  MONITOREAR 👁 (sobrecompra)          │
#   │  MEDIA     │  HUECO    │  REPROGRAMAR ⚡                       │
#   │  MEDIA     │  BAJO     │  REPROGRAMAR ⚡                       │
#   │  MEDIA     │  SALUDABLE│  OK ✅                                │
#   │  MEDIA     │  EXCESO   │  MONITOREAR 👁                        │
#   │  BAJA      │  HUECO    │  MONITOREAR 👁 (producto problema)    │
#   │  BAJA      │  BAJO     │  OK ✅ (poco stock, poca venta)       │
#   │  BAJA      │  SALUDABLE│  LIQUIDAR 📦                          │
#   │  BAJA      │  EXCESO   │  LIQUIDAR 📦 (urgente)               │
#   │  NULA      │  HUECO    │  HUECO ⚪ (sin movimiento)            │
#   │  NULA      │  BAJO     │  MONITOREAR 👁                        │
#   │  NULA      │  SALUDABLE│  LIQUIDAR 📦                          │
#   │  NULA      │  EXCESO   │  LIQUIDAR 📦 (urgente)               │
#   └─────────────────────────────────────────────────────────────────┘
#
#   PRIORIDAD DE REPROGRAMAR:
#   El objetivo principal de Línea Viva es identificar qué se debe reprogramar.
#   REPROGRAMAR aparece cuando hay demanda activa (rotación ALTA o MEDIA)
#   y el stock no alcanza (BAJO o HUECO). Esto captura tanto quiebres
#   totales como situaciones próximas al quiebre.
#
#   VENTAJA vs. SISTEMA ANTERIOR (1 dimensión):
#   Un producto de Alta Rotación con stock bajo ya no "desaparece" dentro
#   de REPROGRAMAR. Ahora aparece en REPROGRAMAR con etiqueta ROT_ALTA,
#   lo que permite priorizarlo sobre un producto de Rotación Media también
#   en REPROGRAMAR. El dato de rotación no se pierde.
#
# ══════════════════════════════════════════════════════════════════════════════

# ─── UMBRALES DE ROTACIÓN (unidades vendidas en 60 días) ──────────────────────
ROT_ALTA   = 10   # >= 10u en 60d → Alta Rotación   (≈ 5+ u/mes)
ROT_MEDIA  = 4    # >=  4u en 60d → Media Rotación  (≈ 2+ u/mes)
ROT_BAJA   = 1    # >=  1u en 60d → Baja Rotación   (algo se vende)
# < ROT_BAJA (== 0)              → Nula (sin ninguna venta en 60d)

# ─── UMBRALES DE STOCK (días de cobertura) ────────────────────────────────────
STOCK_EXCESO    = 120   # >= 120d de cobertura → Exceso
STOCK_SALUDABLE = 30    # >=  30d de cobertura → Saludable
#  > 0 días                      → Bajo
# == 0 (sin stock)               → Hueco

# ─── CONSTANTES OPERATIVAS ────────────────────────────────────────────────────
LEAD_TIME_DIAS = 30    # días que tarda en llegar un pedido (para sugerir reorden)
DIAS_OBJETIVO  = 60    # días de cobertura que se quiere tener después del pedido
MULTIPLO       = 6     # cantidad mínima de reposición y múltiplo de pedido

# ─── CONSTANTES SHOPIFY ───────────────────────────────────────────────────────
LOCATIONS_EXCLUIR = ["Recogida en tienda (NO USAR)"]
LOCATIONS_VALIDAS = ["TERRET", "Tienda Fisica", "Tienda Móvil - Ferias"]
API_VERSION       = "2024-10"
UMBRAL_BS         = 25   # ventas60d >= este valor → producto "Best Seller" (tag visual)


# ──────────────────────────────────────────────────────────────────────────────
#   FUNCIONES DE CLASIFICACIÓN
# ──────────────────────────────────────────────────────────────────────────────

def clasificar_rotacion(ventas60d: float) -> str:
    """
    DIMENSIÓN 1 — ROTACIÓN
    Evalúa únicamente la velocidad de venta. No considera stock.
    Retorna: "ALTA" | "MEDIA" | "BAJA" | "NULA"
    """
    v = float(ventas60d)
    if v >= ROT_ALTA:
        return "ALTA"
    if v >= ROT_MEDIA:
        return "MEDIA"
    if v >= ROT_BAJA:
        return "BAJA"
    return "NULA"


def clasificar_stock(stock: float, ventas60d: float) -> str:
    """
    DIMENSIÓN 2 — STOCK
    Evalúa los días de cobertura disponibles.
    Si no hay ventas de referencia, stock > 0 se trata como EXCESO (sin demanda conocida).
    Retorna: "EXCESO" | "SALUDABLE" | "BAJO" | "HUECO"
    """
    s = float(stock)
    v = float(ventas60d)

    if s <= 0:
        return "HUECO"

    if v <= 0:
        # Stock positivo pero sin ventas → no hay referencia, se trata como exceso
        return "EXCESO"

    dias = s / (v / 60.0)

    if dias >= STOCK_EXCESO:
        return "EXCESO"
    if dias >= STOCK_SALUDABLE:
        return "SALUDABLE"
    return "BAJO"


def clasificar_accion(rotacion: str, stock_nivel: str) -> str:
    """
    DIMENSIÓN 3 — ACCIÓN (derivada de Rotación × Stock)
    Esta función implementa la tabla de decisión documentada arriba.
    NO modificar esta función sin actualizar la tabla en la guía.
    Retorna: "REPROGRAMAR" | "OK" | "MONITOREAR" | "LIQUIDAR" | "HUECO"
    """
    # Casos de REPROGRAMAR: hay demanda pero no hay stock
    if rotacion in ("ALTA", "MEDIA") and stock_nivel in ("BAJO", "HUECO"):
        return "REPROGRAMAR"

    # Casos OK: hay demanda y el stock es suficiente
    if rotacion in ("ALTA", "MEDIA") and stock_nivel == "SALUDABLE":
        return "OK"

    # Sobrecompra con rotación activa → monitorear, no liquidar
    if rotacion in ("ALTA", "MEDIA") and stock_nivel == "EXCESO":
        return "MONITOREAR"

    # Rotación baja con stock bajo → no urgente, pero vigilar
    if rotacion == "BAJA" and stock_nivel == "BAJO":
        return "OK"

    # Producto problemático: vende poco pero no tiene stock
    if rotacion == "BAJA" and stock_nivel == "HUECO":
        return "MONITOREAR"

    # Stock con poca o nula demanda → candidato a liquidar
    if rotacion in ("BAJA", "NULA") and stock_nivel in ("SALUDABLE", "EXCESO"):
        return "LIQUIDAR"

    # Sin stock y sin ventas → producto posiblemente descontinuado
    if rotacion == "NULA" and stock_nivel == "HUECO":
        return "HUECO"

    # Sin ventas pero con stock bajo → monitorear
    if rotacion == "NULA" and stock_nivel == "BAJO":
        return "MONITOREAR"

    # Fallback (no debería llegar aquí si la tabla está completa)
    return "MONITOREAR"


def clasificar_producto(stock: float, ventas60d: float) -> dict:
    """
    Función principal de clasificación. Evalúa las 3 dimensiones y retorna
    un diccionario con toda la información de clasificación del producto.

    Ejemplo de retorno:
    {
        "rotacion":    "ALTA",        # velocidad de venta
        "stock_nivel": "BAJO",        # nivel de cobertura
        "accion":      "REPROGRAMAR", # qué hacer con el producto
        "dias_inv":    18.5,          # días de inventario disponibles
        "es_bs":       True,          # True si ventas >= UMBRAL_BS (best seller)
    }
    """
    try:
        s = float(stock)
        v = float(ventas60d)
    except (ValueError, TypeError):
        return {"rotacion": "NULA", "stock_nivel": "HUECO", "accion": "HUECO",
                "dias_inv": 9999, "es_bs": False}

    dias = round(s / (v / 60.0), 1) if v > 0 else 9999

    rotacion    = clasificar_rotacion(v)
    stock_nivel = clasificar_stock(s, v)
    accion      = clasificar_accion(rotacion, stock_nivel)

    return {
        "rotacion":    rotacion,
        "stock_nivel": stock_nivel,
        "accion":      accion,
        "dias_inv":    dias,
        "es_bs":       v >= UMBRAL_BS,
    }


# ──────────────────────────────────────────────────────────────────────────────
#   CONFIGURACIÓN VISUAL DE DIMENSIONES
# ──────────────────────────────────────────────────────────────────────────────

# Rotación — etiquetas y colores para la UI
ROTACION_CFG = {
    "ALTA":  {"label": "Alta Rotación",  "color": "#2D6A4F", "icon": "🔥"},
    "MEDIA": {"label": "Media Rotación", "color": "#4488FF", "icon": "📦"},
    "BAJA":  {"label": "Baja Rotación",  "color": "#FFB800", "icon": "🐢"},
    "NULA":  {"label": "Sin Ventas",     "color": "#B8B0A4", "icon": "⚪"},
}

# Stock — etiquetas y colores para la UI
STOCK_CFG = {
    "EXCESO":    {"label": "Exceso",    "color": "#FF6B35", "icon": "🔴"},
    "SALUDABLE": {"label": "Saludable", "color": "#00C853", "icon": "✅"},
    "BAJO":      {"label": "Bajo",      "color": "#FFB800", "icon": "⚠️"},
    "HUECO":     {"label": "Hueco",     "color": "#FF3B30", "icon": "❌"},
}

# Acciones — etiquetas, colores y descripciones para la UI
ACCION_CFG = {
    "REPROGRAMAR": {
        "icon":  "⚡",
        "label": "Reprogramar",
        "color": "#FF3B30",
        "desc":  "Hay demanda activa pero el stock no alcanza. Pedir ya.",
    },
    "OK": {
        "icon":  "✅",
        "label": "OK",
        "color": "#2D6A4F",
        "desc":  "Stock y ventas en equilibrio. Sin acción requerida.",
    },
    "MONITOREAR": {
        "icon":  "👁",
        "label": "Monitorear",
        "color": "#4488FF",
        "desc":  "Situación no crítica pero requiere revisión en el próximo ciclo.",
    },
    "LIQUIDAR": {
        "icon":  "📦",
        "label": "Liquidar",
        "color": "#FF9500",
        "desc":  "Stock acumulado con poca o ninguna demanda. Precio especial o retiro.",
    },
    "HUECO": {
        "icon":  "⚪",
        "label": "Hueco",
        "color": "#B8B0A4",
        "desc":  "Sin stock y sin ventas. Posiblemente descontinuado.",
    },
}

# Orden de visualización en el sidebar (de mayor a menor prioridad)
ACCIONES_ORDEN = ["REPROGRAMAR", "OK", "MONITOREAR", "LIQUIDAR", "HUECO"]

# Alias para compatibilidad con código anterior que usaba ESTADOS
# La clave es la acción, el valor es la config visual
ESTADOS = {k: v for k, v in ACCION_CFG.items()}
ORDEN_SIDEBAR = ACCIONES_ORDEN


# ─── PREFIJOS SKU ─────────────────────────────────────────────────────────────
PREFIX_MAP = {
    "Bandana":                  "BAN",
    "Bikers":                   "BKS",
    "Buzo":                     "BUZ",
    "Camiseta":                 "CAM",
    "Cinturón":                 "CIN",
    "Crop top":                 "CRT",
    "Esqueleto":                "ESQ",
    "Gift Cards":               "GFC",
    "Hydratation Flask":        "HFL",
    "Jersey Ciclismo":          "JRS",
    "Leggings":                 "LEG",
    "Manguillas":               "MAN",
    "Medias de Compresión":     "MCO",
    "Medias Tobilleras":        "MTO",
    "Pantalonetas de Ciclismo": "PTC",
    "Pantorrilleras":           "PNT",
    "Short":                    "SHT",
    "Top":                      "TOP",
    "Trisuit":                  "TRI",
    "Vestido de Baño":          "VBA",
    "Visera":                   "VSR",
}


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
[data-testid="stRadio"] label { color: #1A1A14 !important; }
[data-testid="stRadio"] p { color: #1A1A14 !important; }
[data-testid="stRadio"] span { color: #1A1A14 !important; }
div[role="radiogroup"] label { color: #1A1A14 !important; }
div[role="radiogroup"] p { color: #1A1A14 !important; }
[data-testid="stSelectbox"] label { color: #1A1A14 !important; }
[data-testid="stSelectbox"] p { color: #1A1A14 !important; }
label[data-testid="stWidgetLabel"] { color: #1A1A14 !important; }
label[data-testid="stWidgetLabel"] p { color: #1A1A14 !important; }
[data-testid="stToggle"] label { color: #1A1A14 !important; }
[data-testid="stToggle"] p { color: #1A1A14 !important; }
[data-testid="stAppViewContainer"] p { color: #1A1A14 !important; }
[data-testid="stAppViewContainer"] span { color: #1A1A14 !important; }
[data-baseweb="radio"] div { border-color: #2D6A4F !important; }
[data-baseweb="radio"] [aria-checked="true"] div { background: #2D6A4F !important; }
[data-testid="stDataFrame"] { background: #EDEAE0 !important; }
[data-testid="stDataFrame"] iframe { background: #EDEAE0 !important; }
.stDataFrame { background: #EDEAE0 !important; }
</style>
""", unsafe_allow_html=True)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def color_accion(accion):
    return ACCION_CFG.get(accion, {}).get("color", "#B8B0A4")


def color_estado(estado):
    """Alias para compatibilidad con código que usa el nombre anterior."""
    return color_accion(estado)


def sugerir_cantidad(stock, ventas60d, dias_inv, accion):
    try:
        s = float(stock)
        v = float(ventas60d)
        d = float(dias_inv) if str(dias_inv).lower() not in ("inf", "nan", "") else 9999
    except Exception:
        return 0, "Sin datos"
    if accion in ("LIQUIDAR", "HUECO"):
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
                st.markdown(
                    "<div style='max-width:600px;margin:60px auto;'>"
                    "<div style='background:#2D6A4F;color:#F5F0E8;border-radius:8px 8px 0 0;"
                    "padding:16px 20px;font-family:Bebas Neue,sans-serif;font-size:18px;"
                    "letter-spacing:2px;'>✅ SHOPIFY CONECTADO</div>"
                    "<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
                    "border-radius:0 0 8px 8px;padding:20px;'>"
                    "<div style='font-size:13px;color:#1A1A14;margin-bottom:12px;'>"
                    "Copia este token y agrégalo a los Secrets de Streamlit Cloud "
                    "como <b>SHOPIFY_ACCESS_TOKEN</b>:</div>"
                    f"<div style='background:#1A1A14;color:#2DFF6E;font-family:DM Mono,monospace;"
                    f"font-size:13px;padding:14px 16px;border-radius:6px;"
                    f"word-break:break-all;user-select:all;'>{tok}</div>"
                    "<div style='font-size:11px;color:#6B6456;margin-top:12px;'>"
                    "Después de agregarlo en Secrets → Redeployar la app → Listo, no lo necesitas más.</div>"
                    "</div></div>",
                    unsafe_allow_html=True,
                )
                st.stop()
        st.error("Error al obtener token. Intenta de nuevo.")
        st.query_params.clear()
        st.stop()

    scopes   = "read_products,read_inventory,read_locations,read_orders,write_products"
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
    seen_ids = set()
    while url:
        resp = requests.get(url, headers=_headers(token), params=p, timeout=30)
        resp.raise_for_status()
        for item in resp.json().get(key, []):
            item_id = item.get("id")
            if item_id and item_id in seen_ids:
                continue
            if item_id:
                seen_ids.add(item_id)
            results.append(item)
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
                    "variant_id":        v_id,
                    "variant_title":     v["title"],
                    "sku":               v.get("sku", ""),
                    "price":             float(v.get("price", 0) or 0),
                    "cost":              cost,
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


def cargar_ventas_60d(_token, _locations):
    loc_id_to_name = {str(loc["id"]): loc["name"] for loc in _locations}
    ONLINE = "TERRET"

    desde = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%dT%H:%M:%SZ")
    GQL = """
    query($cursor: String, $query: String) {
      orders(first: 250, after: $cursor, query: $query) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id
            cancelledAt
            physicalLocation { id name }
            lineItems(first: 250) {
              edges {
                node {
                  id
                  quantity
                  variant { id }
                }
              }
            }
          }
        }
      }
    }
    """
    query_str = f"created_at:>={desde}"
    ventas_global  = {}
    ventas_por_loc = {}
    cursor = None

    while True:
        data = graphql_query(_token, GQL, {"cursor": cursor, "query": query_str})
        orders_data = data.get("data", {}).get("orders", {})
        for edge in orders_data.get("edges", []):
            node = edge["node"]
            if node.get("cancelledAt"):
                continue
            loc_gid  = (node.get("physicalLocation") or {}).get("id", "")
            loc_id   = loc_gid.split("/")[-1] if loc_gid else ""
            loc_name = loc_id_to_name.get(loc_id, ONLINE)

            seen_line_ids = set()
            for li_edge in node.get("lineItems", {}).get("edges", []):
                li = li_edge["node"]
                line_id = li.get("id", "")
                if line_id in seen_line_ids:
                    continue
                seen_line_ids.add(line_id)
                variant = li.get("variant") or {}
                vid = variant.get("id", "").split("/")[-1]
                if not vid:
                    continue
                qty = int(li.get("quantity") or 0)
                if qty <= 0:
                    continue
                ventas_global[vid] = ventas_global.get(vid, 0) + qty
                if vid not in ventas_por_loc:
                    ventas_por_loc[vid] = {}
                ventas_por_loc[vid][loc_name] = ventas_por_loc[vid].get(loc_name, 0) + qty

        if not orders_data.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = orders_data["pageInfo"]["endCursor"]

    return ventas_global, ventas_por_loc


def cargar_ventas_rango(_token, fecha_desde, fecha_hasta):
    """Usa ShopifyQL directamente — mismos números que el dashboard de Shopify."""
    desde_str = fecha_desde.strftime("%Y-%m-%d")
    hasta_str = fecha_hasta.strftime("%Y-%m-%d")

    shopify_ql = (
        f"FROM sales, discounts, inventory "
        f"SHOW product_title, day, total_sales, quantity_ordered "
        f"GROUP BY product_title, day "
        f"SINCE {desde_str} UNTIL {hasta_str} "
        f"ORDER BY total_sales DESC LIMIT 500"
    )

    GQL = """
    query($query: String!) {
      shopifyqlQuery(query: $query) {
        __typename
        ... on TableResponse {
          tableData {
            unformattedData
            columns { name dataType }
          }
        }
        ... on ParseErrorResponse {
          parseErrors { code message range { start { line column } end { line column } } }
        }
      }
    }
    """
    shop = st.secrets["TIENDA_URL"]
    resp = requests.post(
        f"https://{shop}/admin/api/{API_VERSION}/graphql.json",
        headers=_headers(_token),
        json={"query": GQL, "variables": {"query": shopify_ql}},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()

    result   = (data.get("data") or {}).get("shopifyqlQuery") or {}
    typename = result.get("__typename", "")

    if typename != "TableResponse":
        return _cargar_ventas_rest(_token, fecha_desde, fecha_hasta)

    table    = result.get("tableData") or {}
    columns  = [c["name"] for c in (table.get("columns") or [])]
    rows_raw = table.get("unformattedData") or []

    if not rows_raw:
        return _cargar_ventas_rest(_token, fecha_desde, fecha_hasta)

    rows = []
    for row in rows_raw:
        if len(row) < len(columns):
            continue
        rec = dict(zip(columns, row))
        rows.append({
            "fecha":    str(rec.get("day", desde_str))[:10],
            "producto": str(rec.get("product_title", "")),
            "variante": "",
            "sku":      "",
            "cantidad": int(float(rec.get("quantity_ordered", 0) or 0)),
            "precio":   0,
            "total":    float(rec.get("total_sales", 0) or 0),
        })
    return pd.DataFrame(rows)


def _cargar_ventas_rest(_token, fecha_desde, fecha_hasta):
    shop      = st.secrets["TIENDA_URL"]
    headers   = _headers(_token)
    desde_str = fecha_desde.strftime("%Y-%m-%d")
    hasta_str = fecha_hasta.strftime("%Y-%m-%d")

    shopify_ql = (
        f"FROM sales "
        f"SHOW product_title, total_sales, quantity_ordered "
        f"GROUP BY product_title "
        f"SINCE {desde_str} UNTIL {hasta_str} "
        f"ORDER BY total_sales DESC LIMIT 500"
    )

    try:
        resp = requests.post(
            f"https://{shop}/admin/api/{API_VERSION}/analytics/queries/run.json",
            headers=headers,
            json={"query": shopify_ql},
            timeout=60,
        )
        if resp.status_code == 200:
            data     = resp.json()
            result   = data.get("query_result") or data.get("result") or {}
            cols     = result.get("columns") or []
            rows_raw = result.get("rows") or []
            if cols and rows_raw:
                col_names = [c.get("name", c) if isinstance(c, dict) else c for c in cols]
                rows = []
                for row in rows_raw:
                    rec = dict(zip(col_names, row))
                    rows.append({
                        "fecha":    desde_str,
                        "producto": str(rec.get("product_title", "")),
                        "variante": "",
                        "sku":      "",
                        "cantidad": int(float(rec.get("quantity_ordered", 0) or 0)),
                        "precio":   0,
                        "total":    float(rec.get("total_sales", 0) or 0),
                    })
                return pd.DataFrame(rows)
    except Exception:
        pass

    desde = fecha_desde.strftime("%Y-%m-%dT00:00:00Z")
    hasta = fecha_hasta.strftime("%Y-%m-%dT23:59:59Z")
    GQL = """
    query($cursor: String, $query: String) {
      orders(first: 250, after: $cursor, query: $query) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            id createdAt cancelledAt sourceName
            lineItems(first: 250) {
              edges {
                node {
                  id title variantTitle sku quantity
                  originalUnitPriceSet { shopMoney { amount } }
                }
              }
            }
          }
        }
      }
    }
    """
    query_str = f"created_at:>={desde} created_at:<={hasta}"
    rows   = []
    cursor = None

    CANAL_MAP = {
        "web":    "Online Store",
        "shopify_draft_order": "Draft Orders",
        "pos":    "Point of Sale",
        "iphone": "Online Store",
        "android":"Online Store",
        None:     "Online Store",
    }

    while True:
        data        = graphql_query(_token, GQL, {"cursor": cursor, "query": query_str})
        orders_data = data.get("data", {}).get("orders", {})
        for edge in orders_data.get("edges", []):
            node = edge["node"]
            if node.get("cancelledAt"):
                continue
            fecha  = node.get("createdAt", "")[:10]
            source = node.get("sourceName") or ""
            canal  = CANAL_MAP.get(source.lower() if source else None,
                     "Draft Orders" if "draft" in source.lower() else
                     "Point of Sale" if "pos"   in source.lower() else
                     "Online Store")
            seen_line_ids = set()
            for li_edge in node.get("lineItems", {}).get("edges", []):
                li      = li_edge["node"]
                line_id = li.get("id", "")
                if line_id in seen_line_ids:
                    continue
                seen_line_ids.add(line_id)
                qty = int(li.get("quantity") or 0)
                if qty <= 0:
                    continue
                unit = float((li.get("originalUnitPriceSet") or {}).get("shopMoney", {}).get("amount", 0) or 0)
                rows.append({
                    "fecha":    fecha,
                    "canal":    canal,
                    "producto": li.get("title", ""),
                    "variante": li.get("variantTitle", ""),
                    "sku":      li.get("sku", ""),
                    "cantidad": qty,
                    "precio":   unit,
                    "total":    unit * qty,
                })
        if not orders_data.get("pageInfo", {}).get("hasNextPage"):
            break
        cursor = orders_data["pageInfo"]["endCursor"]
    return pd.DataFrame(rows)


def construir_df(productos, stock_map, ventas_map, locations):
    """
    Construye el DataFrame principal del inventario.
    Aplica la clasificación multidimensional a cada variante.

    Columnas añadidas por la clasificación:
      _rotacion    → "ALTA" | "MEDIA" | "BAJA" | "NULA"
      _stock_nivel → "EXCESO" | "SALUDABLE" | "BAJO" | "HUECO"
      _accion      → "REPROGRAMAR" | "OK" | "MONITOREAR" | "LIQUIDAR" | "HUECO"
      _estado      → alias de _accion (compatibilidad con código anterior)
      _bs          → True si ventas60d >= UMBRAL_BS
    """
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

            ventas60d = ventas_global.get(vid, 0)
            v_por_loc = ventas_por_loc.get(vid, {})
            dias_inv  = round(stock_total / (ventas60d / 60), 1) if ventas60d > 0 else 9999

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
            for loc_id, loc_name in loc_id_to_name.items():
                loc_data = loc_stocks.get(loc_id, {"available": 0, "on_hand": 0})
                row[f"Stock_{loc_name}"]  = loc_data["available"]
                row[f"Fisico_{loc_name}"] = loc_data["on_hand"]
            for loc_name in loc_id_to_name.values():
                row[f"Ventas_{loc_name}"] = v_por_loc.get(loc_name, 0)
            row["Ventas_TERRET"] = v_por_loc.get("TERRET", 0)
            rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # ── Aplicar clasificación multidimensional ─────────────────────────────
    clasificaciones = df.apply(
        lambda r: clasificar_producto(r["Stock"], r["Ventas60d"]), axis=1
    )
    df["_rotacion"]    = clasificaciones.apply(lambda c: c["rotacion"])
    df["_stock_nivel"] = clasificaciones.apply(lambda c: c["stock_nivel"])
    df["_accion"]      = clasificaciones.apply(lambda c: c["accion"])
    df["_estado"]      = df["_accion"]   # alias para compatibilidad
    df["_bs"]          = clasificaciones.apply(lambda c: c["es_bs"])
    df["_valor_costo"] = df["Stock"] * df["Costo"]
    df["_valor_venta"] = df["Stock"] * df["Precio Venta"]

    return df


# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

def render_guia_flotante():
    """
    Guía de clasificación siempre accesible desde el sidebar.
    El botón vive en el sidebar (nativo Streamlit) y el panel se renderiza
    en el área principal usando st.session_state como toggle.
    Streamlit no permite manipular el DOM principal con JS desde iframes,
    por eso el enfoque es renderizado condicional en Python.
    """
    # ── Toggle via session_state ───────────────────────────────────────────────
    if "guia_abierta" not in st.session_state:
        st.session_state.guia_abierta = False

    with st.sidebar:
        st.markdown("<hr style='border-color:#D4CFC4;margin:6px 0;'>", unsafe_allow_html=True)
        label = "✕  Cerrar guía" if st.session_state.guia_abierta else "📖  Guía de clasificación"
        if st.button(label, key="btn_guia_toggle"):
            st.session_state.guia_abierta = not st.session_state.guia_abierta
            st.rerun()

    # ── Panel: solo se renderiza cuando está abierto ───────────────────────────
    if not st.session_state.guia_abierta:
        return

    st.markdown(f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');
    .lv-g-wrap {{
        background: #F5F0E8;
        border: 1px solid #D4CFC4;
        border-radius: 12px;
        overflow: hidden;
        margin-bottom: 32px;
        font-family: 'DM Sans', sans-serif;
        color: #1A1A14;
    }}
    .lv-g-header {{
        background: #2D6A4F;
        padding: 20px 28px 18px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }}
    .lv-g-header-title {{
        font-family: 'Bebas Neue', sans-serif;
        font-size: 22px;
        letter-spacing: 3px;
        color: #F5F0E8;
        line-height: 1;
    }}
    .lv-g-header-sub {{
        font-size: 10px;
        color: rgba(245,240,232,0.6);
        letter-spacing: 1.5px;
        text-transform: uppercase;
        margin-top: 4px;
    }}
    .lv-g-body {{
        padding: 28px 32px 36px;
    }}
    .lv-g-intro {{
        font-size: 13px;
        color: #6B6456;
        line-height: 1.65;
        margin-bottom: 28px;
        padding-bottom: 20px;
        border-bottom: 1px solid #D4CFC4;
    }}
    .lv-g-intro strong {{ color: #1A1A14; }}
    .lv-g-seccion {{ margin-bottom: 32px; }}
    .lv-g-seccion-title {{
        font-family: 'Bebas Neue', sans-serif;
        font-size: 15px;
        letter-spacing: 3px;
        color: #1A1A14;
        margin-bottom: 10px;
        padding-bottom: 7px;
        border-bottom: 2px solid #D4CFC4;
    }}
    .lv-g-dim-desc {{
        font-size: 13px;
        color: #6B6456;
        margin-bottom: 14px;
        line-height: 1.55;
    }}
    .lv-g-dim-desc strong {{ color: #1A1A14; }}
    .lv-g-chips {{
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-bottom: 4px;
    }}
    .lv-g-chip {{
        display: inline-flex;
        flex-direction: column;
        padding: 8px 14px;
        border-radius: 8px;
        font-size: 13px;
        font-weight: 600;
        border: 1.5px solid;
        min-width: 140px;
    }}
    .lv-g-chip-sub {{
        font-family: 'DM Mono', monospace;
        font-size: 10px;
        font-weight: 400;
        opacity: 0.7;
        margin-top: 3px;
    }}
    .lv-g-table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
        margin-top: 6px;
        background: #EDEAE0;
        border-radius: 8px;
        overflow: hidden;
    }}
    .lv-g-table th {{
        font-family: 'DM Mono', monospace;
        font-size: 9px;
        letter-spacing: 1.5px;
        text-transform: uppercase;
        color: #B8B0A4;
        font-weight: 400;
        padding: 10px 14px;
        text-align: left;
        background: #E5E0D6;
        border-bottom: 1px solid #D4CFC4;
    }}
    .lv-g-table td {{
        padding: 10px 14px;
        border-bottom: 1px solid #D4CFC4;
        vertical-align: middle;
        color: #1A1A14;
        line-height: 1.4;
        background: #EDEAE0;
    }}
    .lv-g-table tr:last-child td {{ border-bottom: none; }}
    .lv-g-table tr:nth-child(even) td {{ background: #E8E3D8; }}
    .lv-g-badge {{
        display: inline-block;
        padding: 4px 10px;
        border-radius: 10px;
        font-size: 12px;
        font-weight: 600;
        white-space: nowrap;
    }}
    .lv-g-block {{
        background: #EDEAE0;
        border: 1px solid #D4CFC4;
        border-radius: 8px;
        padding: 16px 20px;
        margin-top: 10px;
    }}
    .lv-g-block-title {{
        font-family: 'Bebas Neue', sans-serif;
        font-size: 11px;
        letter-spacing: 2px;
        color: #6B6456;
        margin-bottom: 12px;
    }}
    .lv-g-um-row {{
        display: grid;
        grid-template-columns: 160px 120px 1fr;
        align-items: baseline;
        gap: 10px;
        margin-bottom: 8px;
        font-size: 13px;
    }}
    .lv-g-um-name {{ font-weight: 600; }}
    .lv-g-um-val {{
        font-family: 'DM Mono', monospace;
        font-size: 13px;
        font-weight: 600;
        color: #2D6A4F;
    }}
    .lv-g-um-desc {{ color: #6B6456; font-size: 12px; }}
    .lv-g-nota {{
        background: #EDEAE0;
        border: 1px solid #D4CFC4;
        border-left: 4px solid #FFB800;
        border-radius: 6px;
        padding: 12px 16px;
        font-size: 12px;
        color: #6B6456;
        line-height: 1.55;
        margin-top: 18px;
    }}
    .lv-g-nota strong {{ color: #1A1A14; }}
    </style>

    <div class="lv-g-wrap">
        <div class="lv-g-header">
            <div>
                <div class="lv-g-header-title">GUÍA DE CLASIFICACIÓN</div>
                <div class="lv-g-header-sub">Línea Viva v9 · Sistema multidimensional · Térret</div>
            </div>
        </div>
        <div class="lv-g-body">

            <!-- INTRO -->
            <div class="lv-g-intro">
                Cada producto se evalúa en <strong>3 dimensiones independientes</strong>.
                Las dos primeras (Rotación y Stock) se calculan con datos reales de Shopify.
                La tercera (Acción) es la conclusión lógica que resulta de cruzar las dos anteriores —
                no se configura directamente, se <em>deriva</em>.<br><br>
                El objetivo principal de Línea Viva es tener siempre claro
                <strong>qué productos se deben reprogramar</strong>: aquellos que tienen
                demanda activa pero stock insuficiente para cubrirla.
                Las otras acciones (Liquidar, Monitorear) son información complementaria
                que ayuda a tomar decisiones sobre el resto del catálogo.
            </div>

            <!-- DIM 1: ROTACIÓN -->
            <div class="lv-g-seccion">
                <div class="lv-g-seccion-title">DIMENSIÓN 1 — ROTACIÓN</div>
                <div class="lv-g-dim-desc">
                    <strong>Pregunta:</strong> ¿cuánto vende este producto?<br>
                    <strong>Fuente:</strong> unidades vendidas en los últimos 60 días.<br>
                    No depende del stock — solo del comportamiento de venta histórico.
                    Un producto puede tener Alta Rotación aunque esté en quiebre.
                </div>
                <div class="lv-g-chips">
                    <div class="lv-g-chip" style="color:#2D6A4F;border-color:#2D6A4F;background:rgba(45,106,79,0.08);">
                        🔥 Alta Rotación
                        <span class="lv-g-chip-sub">≥ {ROT_ALTA} u en 60d · ≈ {round(ROT_ALTA/2,1)}+ u/mes</span>
                    </div>
                    <div class="lv-g-chip" style="color:#4488FF;border-color:#4488FF;background:rgba(68,136,255,0.08);">
                        📦 Media Rotación
                        <span class="lv-g-chip-sub">≥ {ROT_MEDIA} u en 60d · ≈ {round(ROT_MEDIA/2,1)}+ u/mes</span>
                    </div>
                    <div class="lv-g-chip" style="color:#B8860B;border-color:#FFB800;background:rgba(255,184,0,0.08);">
                        🐢 Baja Rotación
                        <span class="lv-g-chip-sub">≥ {ROT_BAJA} u en 60d · algo se vende</span>
                    </div>
                    <div class="lv-g-chip" style="color:#8A8278;border-color:#B8B0A4;background:rgba(184,176,164,0.12);">
                        ⚪ Sin Ventas
                        <span class="lv-g-chip-sub">0 u en 60d · sin demanda registrada</span>
                    </div>
                </div>
            </div>

            <!-- DIM 2: STOCK -->
            <div class="lv-g-seccion">
                <div class="lv-g-seccion-title">DIMENSIÓN 2 — STOCK</div>
                <div class="lv-g-dim-desc">
                    <strong>Pregunta:</strong> ¿cuántos días de inventario quedan?<br>
                    <strong>Fórmula:</strong> stock disponible ÷ (ventas 60d ÷ 60) = días de cobertura.<br>
                    Si el producto no tiene ventas registradas, el stock se clasifica como
                    Exceso porque no hay referencia de demanda con qué compararlo.
                </div>
                <div class="lv-g-chips">
                    <div class="lv-g-chip" style="color:#CC4A1A;border-color:#FF6B35;background:rgba(255,107,53,0.08);">
                        🔴 Exceso
                        <span class="lv-g-chip-sub">≥ {STOCK_EXCESO}d de cobertura · +{round(STOCK_EXCESO/30,0):.0f} meses</span>
                    </div>
                    <div class="lv-g-chip" style="color:#007A32;border-color:#00C853;background:rgba(0,200,83,0.08);">
                        ✅ Saludable
                        <span class="lv-g-chip-sub">≥ {STOCK_SALUDABLE}d de cobertura · zona ideal</span>
                    </div>
                    <div class="lv-g-chip" style="color:#B8860B;border-color:#FFB800;background:rgba(255,184,0,0.08);">
                        ⚠️ Bajo
                        <span class="lv-g-chip-sub">&gt; 0d pero &lt; {STOCK_SALUDABLE}d · atención</span>
                    </div>
                    <div class="lv-g-chip" style="color:#CC1A1A;border-color:#FF3B30;background:rgba(255,59,48,0.08);">
                        ❌ Hueco
                        <span class="lv-g-chip-sub">Stock = 0 · quiebre total</span>
                    </div>
                </div>
            </div>

            <!-- DIM 3: ACCIÓN -->
            <div class="lv-g-seccion">
                <div class="lv-g-seccion-title">DIMENSIÓN 3 — ACCIÓN (derivada automáticamente)</div>
                <div class="lv-g-dim-desc">
                    <strong>Pregunta:</strong> ¿qué hago con este producto?<br>
                    Esta dimensión <strong>no se configura</strong> — es el resultado de cruzar
                    Rotación × Stock según la tabla de decisión. Si se modifican los umbrales
                    de las dimensiones 1 o 2, las acciones se recalculan automáticamente.
                </div>
                <table class="lv-g-table">
                    <thead>
                        <tr>
                            <th>ROTACIÓN</th>
                            <th>STOCK</th>
                            <th>ACCIÓN</th>
                            <th>QUÉ SIGNIFICA</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>🔥 Alta</td>
                            <td>❌ Hueco</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,59,48,0.12);color:#CC1A1A;">⚡ Reprogramar</span></td>
                            <td>Quiebre total — pedir de inmediato</td>
                        </tr>
                        <tr>
                            <td>🔥 Alta</td>
                            <td>⚠️ Bajo</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,59,48,0.12);color:#CC1A1A;">⚡ Reprogramar</span></td>
                            <td>Stock no cubre el tiempo de entrega</td>
                        </tr>
                        <tr>
                            <td>📦 Media</td>
                            <td>❌ Hueco</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,59,48,0.12);color:#CC1A1A;">⚡ Reprogramar</span></td>
                            <td>Sin stock con demanda activa</td>
                        </tr>
                        <tr>
                            <td>📦 Media</td>
                            <td>⚠️ Bajo</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,59,48,0.12);color:#CC1A1A;">⚡ Reprogramar</span></td>
                            <td>El stock se agotará antes de que llegue el pedido</td>
                        </tr>
                        <tr>
                            <td>🔥 Alta</td>
                            <td>✅ Saludable</td>
                            <td><span class="lv-g-badge" style="background:rgba(45,106,79,0.12);color:#1a5c38;">✅ OK</span></td>
                            <td>Equilibrado — sin acción requerida</td>
                        </tr>
                        <tr>
                            <td>📦 Media</td>
                            <td>✅ Saludable</td>
                            <td><span class="lv-g-badge" style="background:rgba(45,106,79,0.12);color:#1a5c38;">✅ OK</span></td>
                            <td>Equilibrado — sin acción requerida</td>
                        </tr>
                        <tr>
                            <td>🐢 Baja</td>
                            <td>⚠️ Bajo</td>
                            <td><span class="lv-g-badge" style="background:rgba(45,106,79,0.12);color:#1a5c38;">✅ OK</span></td>
                            <td>Poca venta y poco stock — están en equilibrio</td>
                        </tr>
                        <tr>
                            <td>🔥 Alta</td>
                            <td>🔴 Exceso</td>
                            <td><span class="lv-g-badge" style="background:rgba(68,136,255,0.12);color:#2255BB;">👁 Monitorear</span></td>
                            <td>Vende bien pero se sobrecompró — no pedir más por ahora</td>
                        </tr>
                        <tr>
                            <td>📦 Media</td>
                            <td>🔴 Exceso</td>
                            <td><span class="lv-g-badge" style="background:rgba(68,136,255,0.12);color:#2255BB;">👁 Monitorear</span></td>
                            <td>Stock muy alto para su ritmo de venta</td>
                        </tr>
                        <tr>
                            <td>🐢 Baja</td>
                            <td>❌ Hueco</td>
                            <td><span class="lv-g-badge" style="background:rgba(68,136,255,0.12);color:#2255BB;">👁 Monitorear</span></td>
                            <td>Producto problema — vende poco y encima no hay stock</td>
                        </tr>
                        <tr>
                            <td>⚪ Sin Ventas</td>
                            <td>⚠️ Bajo</td>
                            <td><span class="lv-g-badge" style="background:rgba(68,136,255,0.12);color:#2255BB;">👁 Monitorear</span></td>
                            <td>Sin demanda registrada — revisar si el producto continúa</td>
                        </tr>
                        <tr>
                            <td>🐢 Baja</td>
                            <td>✅ Saludable</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,149,0,0.12);color:#AA5500;">📦 Liquidar</span></td>
                            <td>Stock acumulado con poca demanda — precio especial</td>
                        </tr>
                        <tr>
                            <td>🐢 Baja</td>
                            <td>🔴 Exceso</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,149,0,0.12);color:#AA5500;">📦 Liquidar</span></td>
                            <td>Mucho stock y casi no vende — liquidar urgente</td>
                        </tr>
                        <tr>
                            <td>⚪ Sin Ventas</td>
                            <td>✅ Saludable</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,149,0,0.12);color:#AA5500;">📦 Liquidar</span></td>
                            <td>No vende pero tiene stock — liberar capital</td>
                        </tr>
                        <tr>
                            <td>⚪ Sin Ventas</td>
                            <td>🔴 Exceso</td>
                            <td><span class="lv-g-badge" style="background:rgba(255,149,0,0.12);color:#AA5500;">📦 Liquidar</span></td>
                            <td>No vende y está sobrecomprado — caso urgente</td>
                        </tr>
                        <tr>
                            <td>⚪ Sin Ventas</td>
                            <td>❌ Hueco</td>
                            <td><span class="lv-g-badge" style="background:rgba(184,176,164,0.2);color:#6B6456;">⚪ Hueco</span></td>
                            <td>Sin stock y sin ventas — posiblemente descontinuado</td>
                        </tr>
                    </tbody>
                </table>
            </div>

            <!-- UMBRALES -->
            <div class="lv-g-seccion">
                <div class="lv-g-seccion-title">UMBRALES ACTUALES</div>
                <div class="lv-g-dim-desc">
                    Valores configurados en el archivo <code style="background:#EDEAE0;padding:1px 5px;border-radius:3px;">linea_viva2.0.py</code>.
                    Modificar solo las constantes al inicio del archivo — un cambio actualiza toda la clasificación automáticamente.
                </div>
                <div class="lv-g-block" style="border-left:4px solid #2D6A4F;">
                    <div class="lv-g-block-title">ROTACIÓN — VENTAS EN 60 DÍAS</div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#2D6A4F;">ROT_ALTA</span>
                        <span class="lv-g-um-val">≥ {ROT_ALTA} u</span>
                        <span class="lv-g-um-desc">Alta Rotación — ≈ {round(ROT_ALTA/2,1)}+ unidades por mes</span>
                    </div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#4488FF;">ROT_MEDIA</span>
                        <span class="lv-g-um-val">≥ {ROT_MEDIA} u</span>
                        <span class="lv-g-um-desc">Media Rotación — ≈ {round(ROT_MEDIA/2,1)}+ unidades por mes</span>
                    </div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#B8860B;">ROT_BAJA</span>
                        <span class="lv-g-um-val">≥ {ROT_BAJA} u</span>
                        <span class="lv-g-um-desc">Baja Rotación — algo se vende en el período</span>
                    </div>
                </div>
                <div class="lv-g-block" style="margin-top:10px;border-left:4px solid #4488FF;">
                    <div class="lv-g-block-title">STOCK — DÍAS DE COBERTURA</div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#CC4A1A;">STOCK_EXCESO</span>
                        <span class="lv-g-um-val">≥ {STOCK_EXCESO}d</span>
                        <span class="lv-g-um-desc">Más de {round(STOCK_EXCESO/30,0):.0f} meses de cobertura — sobrecompra</span>
                    </div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#007A32;">STOCK_SALUDABLE</span>
                        <span class="lv-g-um-val">≥ {STOCK_SALUDABLE}d</span>
                        <span class="lv-g-um-desc">Cobertura mínima recomendada</span>
                    </div>
                </div>
                <div class="lv-g-block" style="margin-top:10px;border-left:4px solid #B8B0A4;">
                    <div class="lv-g-block-title">PARÁMETROS OPERATIVOS</div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#6B6456;">LEAD_TIME_DIAS</span>
                        <span class="lv-g-um-val">{LEAD_TIME_DIAS}d</span>
                        <span class="lv-g-um-desc">Días que tarda en llegar un pedido desde que se hace</span>
                    </div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#6B6456;">DIAS_OBJETIVO</span>
                        <span class="lv-g-um-val">{DIAS_OBJETIVO}d</span>
                        <span class="lv-g-um-desc">Cobertura deseada después de recibir el pedido</span>
                    </div>
                    <div class="lv-g-um-row">
                        <span class="lv-g-um-name" style="color:#6B6456;">MULTIPLO</span>
                        <span class="lv-g-um-val">{MULTIPLO} u</span>
                        <span class="lv-g-um-desc">Cantidad mínima y múltiplo de cada orden de compra</span>
                    </div>
                </div>
            </div>

            <!-- POR QUÉ 3 DIMENSIONES -->
            <div class="lv-g-seccion">
                <div class="lv-g-seccion-title">POR QUÉ 3 DIMENSIONES</div>
                <div class="lv-g-dim-desc">
                    El sistema anterior usaba un solo segmento por producto.
                    El problema: un producto que vende 30 unidades al mes con stock bajo
                    caía en <em>Reprogramar</em> sin que se pudiera distinguir
                    si era más o menos urgente que otro producto de 5 unidades en el mismo estado.<br><br>
                    Con el nuevo sistema, dentro de <strong>Reprogramar</strong> cada producto
                    lleva su etiqueta de rotación visible — lo que permite priorizar:
                    un producto de <strong>Alta Rotación + Hueco</strong> es considerablemente más
                    urgente que uno de <strong>Media Rotación + Bajo</strong>,
                    aunque ambos aparezcan en la misma sección de acción.
                </div>
                <div class="lv-g-nota">
                    ⚠️ <strong>Margen de seguridad actual:</strong> STOCK_SALUDABLE ({STOCK_SALUDABLE}d) − LEAD_TIME ({LEAD_TIME_DIAS}d)
                    = solo <strong>{STOCK_SALUDABLE - LEAD_TIME_DIAS} días de margen</strong> entre que se hace el pedido y que el stock se agota.
                    Si los proveedores demoran más de lo esperado, considera subir <code>STOCK_SALUDABLE</code> a 45 o 60 días.
                </div>
            </div>

        </div><!-- /lv-g-body -->
    </div><!-- /lv-g-wrap -->
    """, unsafe_allow_html=True)


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
            ("SKU_MGR",    "🏷  Gestión de SKUs"),
        ]:
            if st.button(label, key=f"nav_{nav_id}"):
                st.session_state.vista = nav_id
                st.rerun()

        st.markdown(
            "<hr style='border-color:#D4CFC4;margin:6px 0;'>"
            "<div style='font-size:9px;color:#B8B0A4;letter-spacing:1.5px;"
            "text-transform:uppercase;padding:6px 4px 4px 4px;'>Inventario por Acción</div>",
            unsafe_allow_html=True,
        )

        for accion in ACCIONES_ORDEN:
            cfg = ACCION_CFG[accion]
            cnt = conteos.get(accion, 0)
            if st.button(f"{cfg['icon']}  {cfg['label']}   {cnt}", key=f"nav_{accion}"):
                st.session_state.vista = accion
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


def vista_dashboard(df, locations, token):
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

    loc_names = [loc["name"] for loc in locations]
    loc_cols  = [c for c in df.columns if c.startswith("Stock_")]
    df_view   = df.copy()
    sel_loc   = "Todas las sucursales"

    if loc_cols:
        loc_names_filtrados = [n for n in loc_names if n not in LOCATIONS_EXCLUIR]
        fc1, fc2 = st.columns([2, 1])
        with fc1:
            sel_loc = st.selectbox("📍 Filtrar por sucursal:", ["Todas las sucursales"] + loc_names_filtrados, key="dash_loc")
        with fc2:
            tipo_inv = st.radio("📦 Tipo:", ["Disponible", "Físico"], horizontal=True, key="dash_tipo_inv")

        usar_fisico = tipo_inv == "Físico"

        if sel_loc != "Todas las sucursales":
            col_disp   = f"Stock_{sel_loc}"
            col_fisico = f"Fisico_{sel_loc}"
            col_usar   = col_fisico if usar_fisico else col_disp
            if col_usar in df_view.columns:
                df_view["Stock"] = df_view[col_usar].clip(lower=0)
                clases = df_view.apply(
                    lambda r: clasificar_producto(r["Stock"], r["Ventas60d"]), axis=1
                )
                df_view["_rotacion"]    = clases.apply(lambda c: c["rotacion"])
                df_view["_stock_nivel"] = clases.apply(lambda c: c["stock_nivel"])
                df_view["_accion"]      = clases.apply(lambda c: c["accion"])
                df_view["_estado"]      = df_view["_accion"]
                df_view["DiasInv_n"]    = clases.apply(lambda c: c["dias_inv"])
                df_view["_valor_costo"] = df_view["Stock"] * df_view["Costo"]
                df_view["_valor_venta"] = df_view["Stock"] * df_view["Precio Venta"]
        else:
            if usar_fisico:
                df_view["Stock"] = df_view["StockFisico"]
                clases = df_view.apply(
                    lambda r: clasificar_producto(r["Stock"], r["Ventas60d"]), axis=1
                )
                df_view["_rotacion"]    = clases.apply(lambda c: c["rotacion"])
                df_view["_stock_nivel"] = clases.apply(lambda c: c["stock_nivel"])
                df_view["_accion"]      = clases.apply(lambda c: c["accion"])
                df_view["_estado"]      = df_view["_accion"]
                df_view["DiasInv_n"]    = clases.apply(lambda c: c["dias_inv"])
                df_view["_valor_costo"] = df_view["Stock"] * df_view["Costo"]
                df_view["_valor_venta"] = df_view["Stock"] * df_view["Precio Venta"]

        st.markdown("<hr style='border-color:#D4CFC4;margin:10px 0 20px 0;'>", unsafe_allow_html=True)

    tiene_costos  = df_view["Costo"].sum() > 0
    tiene_precios = df_view["Precio Venta"].sum() > 0

    total_skus  = len(df_view)
    total_prods = df_view["Producto"].nunique()
    total_stock = int(df_view["Stock"].sum())
    reprog_n    = int(df_view[df_view["_accion"] == "REPROGRAMAR"]["Producto"].nunique())
    vc          = df_view["_valor_costo"].sum()
    vv          = df_view["_valor_venta"].sum()
    total_comprometido = int(df_view["Comprometido"].sum())

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: st.metric("SKUs",          total_skus)
    with c2: st.metric("Productos",     total_prods)
    with c3: st.metric("Disponible",    f"{total_stock:,}")
    with c4: st.metric("Comprometido",  f"{total_comprometido:,}",
                        help="Unidades reservadas en órdenes pendientes")
    with c5: st.metric("A reprogramar", reprog_n)

    if tiene_costos or tiene_precios:
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Valor inventario (costo)", "$" + f"{vc:,.0f}" if vc > 0 else "—")
        with c2: st.metric("Valor inventario (venta)", "$" + f"{vv:,.0f}" if vv > 0 else "—")
        with c3:
            mg = ((vv - vc) / vc * 100) if vc > 0 else 0
            st.metric("Margen potencial", f"{mg:.1f}%" if mg > 0 else "—")

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

    # ── Ventas 30d por canal ──────────────────────────────────────────────────
    _seccion("VENTAS POR CANAL", "Últimos 30 días · ventas brutas")
    hoy_d   = datetime.now().date()
    desde_d = hoy_d - timedelta(days=30)
    with st.spinner("Cargando ventas por canal..."):
        df_canales = cargar_ventas_rango(token, desde_d, hoy_d)

    if not df_canales.empty and "canal" in df_canales.columns:
        CANAL_COLORES = {
            "Online Store":  "#2D6A4F",
            "Point of Sale": "#4488FF",
            "Draft Orders":  "#FFB800",
        }
        resumen_canal = (
            df_canales.groupby("canal")
            .agg(total=("total", "sum"), unidades=("cantidad", "sum"))
            .reset_index()
            .sort_values("total", ascending=False)
        )
        total_global = resumen_canal["total"].sum()
        cols_canal   = st.columns(len(resumen_canal))
        for i, row in enumerate(resumen_canal.itertuples()):
            color_c = CANAL_COLORES.get(row.canal, "#B8B0A4")
            pct     = round(row.total / total_global * 100, 1) if total_global > 0 else 0
            with cols_canal[i]:
                st.markdown(
                    f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
                    f"border-left:4px solid {color_c};border-radius:6px;padding:12px 14px;'>"
                    f"<div style='font-size:9px;letter-spacing:1.5px;text-transform:uppercase;"
                    f"color:#6B6456;margin-bottom:6px;'>{row.canal}</div>"
                    f"<div style='font-family:Bebas Neue,sans-serif;font-size:1.6rem;"
                    f"color:{color_c};line-height:1;'>{fmt_pesos(row.total)}</div>"
                    f"<div style='font-size:10px;color:#6B6456;margin-top:4px;'>"
                    f"{int(row.unidades):,} u · {pct}% del total</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )
        st.markdown(
            "<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-left:3px solid #FFB800;"
            "border-radius:6px;padding:8px 14px;margin-top:10px;font-size:11px;color:#6B6456;'>"
            "⚠️ <b>Ventas brutas</b> — precio original × unidades. "
            "Desfase estimado vs Shopify: <b>5–15%</b> por descuentos y devoluciones."
            "</div>",
            unsafe_allow_html=True,
        )

    # ── Donut por ACCIÓN + Donut por ROTACIÓN ─────────────────────────────────
    _seccion("VISIÓN GENERAL", "Acciones requeridas y distribución de rotación")

    col_l, col_r = st.columns(2)

    with col_l:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:8px;'>ACCIÓN REQUERIDA</div>",
            unsafe_allow_html=True,
        )
        seg_acc = df_view.groupby("_accion")["Producto"].nunique().reset_index()
        seg_acc.columns = ["Accion", "Productos"]
        seg_acc = seg_acc[seg_acc["Productos"] > 0]

        fig_acc = go.Figure(go.Pie(
            labels=[ACCION_CFG.get(a, {}).get("label", a) for a in seg_acc["Accion"]],
            values=seg_acc["Productos"],
            hole=0.55,
            marker=dict(
                colors=[ACCION_CFG.get(a, {}).get("color", "#B8B0A4") for a in seg_acc["Accion"]],
                line=dict(color="#F5F0E8", width=2),
            ),
            textinfo="label+percent",
            textfont=dict(size=12, color="#1A1A14"),
            hovertemplate="<b>%{label}</b><br>%{value} productos<br>%{percent}<extra></extra>",
        ))
        fig_acc.update_layout(
            **PLOT_BASE,
            margin=dict(t=30, b=30, l=10, r=10),
            height=320,
            showlegend=False,
            annotations=[dict(
                text=f"<b>{total_prods}</b><br>productos",
                x=0.5, y=0.5, font_size=16, showarrow=False,
                font=dict(color="#1A1A14"),
            )],
        )
        st.plotly_chart(fig_acc, use_container_width=True, config={"displayModeBar": False})

    with col_r:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#6B6456;margin-bottom:8px;'>ROTACIÓN DE VENTAS</div>",
            unsafe_allow_html=True,
        )
        seg_rot = df_view.groupby("_rotacion")["Producto"].nunique().reset_index()
        seg_rot.columns = ["Rotacion", "Productos"]
        seg_rot = seg_rot[seg_rot["Productos"] > 0]

        fig_rot = go.Figure(go.Pie(
            labels=[ROTACION_CFG.get(r, {}).get("label", r) for r in seg_rot["Rotacion"]],
            values=seg_rot["Productos"],
            hole=0.55,
            marker=dict(
                colors=[ROTACION_CFG.get(r, {}).get("color", "#B8B0A4") for r in seg_rot["Rotacion"]],
                line=dict(color="#F5F0E8", width=2),
            ),
            textinfo="label+percent",
            textfont=dict(size=12, color="#1A1A14"),
            hovertemplate="<b>%{label}</b><br>%{value} productos<br>%{percent}<extra></extra>",
        ))
        fig_rot.update_layout(
            **PLOT_BASE,
            margin=dict(t=30, b=30, l=10, r=10),
            height=320,
            showlegend=False,
            annotations=[dict(
                text=f"<b>{total_prods}</b><br>productos",
                x=0.5, y=0.5, font_size=16, showarrow=False,
                font=dict(color="#1A1A14"),
            )],
        )
        st.plotly_chart(fig_rot, use_container_width=True, config={"displayModeBar": False})

    # ── Stock crítico (REPROGRAMAR) ────────────────────────────────────────────
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin:20px 0 8px 0;'>STOCK CRÍTICO — TOP 10 A REPROGRAMAR</div>",
        unsafe_allow_html=True,
    )
    criticos = (
        df_view[df_view["_accion"] == "REPROGRAMAR"]
        .groupby("Producto")
        .agg(
            ventas=("Ventas60d", "sum"),
            stock=("Stock", "sum"),
            dias_min=("DiasInv_n", "min"),
            rotacion=("_rotacion", "first"),
        )
        .reset_index()
        .sort_values("ventas", ascending=False)
        .head(10)
        .sort_values("ventas", ascending=True)
    )
    if criticos.empty:
        st.markdown(
            "<div style='text-align:center;padding:40px;color:#6B6456;'>Sin productos críticos</div>",
            unsafe_allow_html=True,
        )
    else:
        def label_crit(row):
            if row["stock"] == 0:
                return "QUIEBRE"
            return f"{int(row['dias_min'])}d"

        criticos["_label"] = criticos["Producto"].apply(lambda x: x[:32] + "..." if len(x) > 32 else x)
        # Color por rotación dentro del segmento REPROGRAMAR
        rot_color_map = {"ALTA": "#FF3B30", "MEDIA": "#FFB800", "BAJA": "#FF6B35", "NULA": "#B8B0A4"}
        fig_crit = go.Figure(go.Bar(
            x=criticos["ventas"],
            y=criticos["_label"],
            orientation="h",
            marker=dict(
                color=criticos["rotacion"].map(rot_color_map).fillna("#FFB800"),
                opacity=0.85,
            ),
            text=criticos.apply(label_crit, axis=1),
            textposition="outside",
            textfont=dict(size=11, color="#1A1A14"),
            hovertemplate="<b>%{y}</b><br>%{x} u vendidas 60d<extra></extra>",
        ))
        fig_crit.update_layout(
            **PLOT_BASE,
            margin=dict(t=10, b=10, l=260, r=100),
            height=360,
            xaxis=dict(showgrid=True, gridcolor="#D4CFC4", zeroline=False, showticklabels=False,
                       range=[0, criticos["ventas"].max() * 1.4]),
            yaxis=dict(showgrid=False, tickfont=dict(size=11, color="#1A1A14"), tickcolor="#1A1A14"),
        )
        # Leyenda manual de colores de rotación
        st.markdown(
            "<div style='display:flex;gap:16px;margin-bottom:4px;font-size:11px;'>"
            "<span style='display:flex;align-items:center;gap:5px;'>"
            "<span style='display:inline-block;width:12px;height:12px;background:#FF3B30;border-radius:2px;'></span>"
            "Alta Rotación</span>"
            "<span style='display:flex;align-items:center;gap:5px;'>"
            "<span style='display:inline-block;width:12px;height:12px;background:#FFB800;border-radius:2px;'></span>"
            "Media Rotación</span>"
            "<span style='display:flex;align-items:center;gap:5px;'>"
            "<span style='display:inline-block;width:12px;height:12px;background:#FF6B35;border-radius:2px;'></span>"
            "Baja Rotación</span>"
            "</div>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(fig_crit, use_container_width=True, config={"displayModeBar": False})

    # ── Top ventas ─────────────────────────────────────────────────────────────
    st.markdown("<div style='height:8px;'></div>", unsafe_allow_html=True)
    _label_loc = f" · {sel_loc}" if sel_loc != "Todas las sucursales" else ""
    _seccion("TOP VENTAS 60D", f"Últimos 60 días{_label_loc}")

    tc1, tc2, tc3 = st.columns([8, 1, 1])
    with tc2:
        n_top = st.select_slider("", options=[10, 15, 20, 30, 50], value=10,
                                 key="slider_top_ventas", label_visibility="collapsed")
    with tc3:
        vista_sku = st.toggle("Por SKU", key="toggle_top_sku", value=False)

    if vista_sku:
        top_data = df_view[["Producto", "Variante", "SKU", "Ventas60d", "_accion", "_rotacion"]].copy()
        top_data = top_data.sort_values("Ventas60d", ascending=True).tail(n_top)
        top_data["etiqueta"] = top_data["SKU"] + "  " + top_data["Variante"].str[:18]
        y_vals   = top_data["etiqueta"].tolist()
        x_vals   = top_data["Ventas60d"].tolist()
        acciones = top_data["_accion"].tolist()
    else:
        col_ventas = f"Ventas_{sel_loc}" if sel_loc != "Todas las sucursales" and f"Ventas_{sel_loc}" in df_view.columns else "Ventas60d"
        top_data = (
            df_view.groupby("_product_id")
            .apply(lambda g: pd.Series({
                "Producto":  g["Producto"].iloc[0],
                "Ventas60d": g[col_ventas].sum(),
                "_accion":   g.loc[g[col_ventas].idxmax(), "_accion"] if g[col_ventas].sum() > 0 else g["_accion"].iloc[0],
            }))
            .reset_index(drop=True)
        )
        top_data = top_data[top_data["Ventas60d"] > 0].sort_values("Ventas60d", ascending=True).tail(n_top)
        y_vals   = top_data["Producto"].tolist()
        x_vals   = top_data["Ventas60d"].tolist()
        acciones = top_data["_accion"].tolist()

    colores_top = [ACCION_CFG.get(a, {}).get("color", "#4488FF") for a in acciones]
    max_x       = max(x_vals) if x_vals else 1
    filas       = list(zip(y_vals, x_vals, colores_top))[::-1]
    filas_html  = "".join(
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

    # ── Stock por categoría ────────────────────────────────────────────────────
    _seccion("STOCK POR CATEGORÍA", "Valor en inventario por línea de producto")
    por_tipo = (
        df_view[df_view["Tipo"].str.strip() != ""]
        .groupby("Tipo")
        .agg(stock=("Stock", "sum"), valor_costo=("_valor_costo", "sum"), valor_venta=("_valor_venta", "sum"))
        .reset_index()
        .sort_values("stock", ascending=True)
    )
    por_tipo = por_tipo[por_tipo["stock"] > 0]
    x_cat    = por_tipo["valor_costo"] if tiene_costos else por_tipo["stock"]
    txt_cat  = ["$" + f"{v:,.0f}" for v in x_cat] if tiene_costos else [str(int(v)) + " u" for v in x_cat]
    max_cat  = x_cat.max() if len(x_cat) > 0 else 1
    cat_filas = list(zip(por_tipo["Tipo"].tolist()[::-1], x_cat.tolist()[::-1], txt_cat[::-1]))
    cat_html  = "".join(
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

    # ── Resumen por acción ─────────────────────────────────────────────────────
    _seccion("RESUMEN POR ACCIÓN")
    resumen = []
    for accion in ACCIONES_ORDEN:
        cfg = ACCION_CFG[accion]
        sub = df_view[df_view["_accion"] == accion]
        if sub.empty:
            continue
        row = {
            "Acción":      cfg["icon"] + " " + cfg["label"],
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
    if resumen:
        st.dataframe(pd.DataFrame(resumen), use_container_width=True, hide_index=True)


# ─── MÓDULO 1: INVENTARIO POR ACCIÓN ─────────────────────────────────────────

def vista_inventario(df, accion, locations):
    cfg   = ACCION_CFG[accion]
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

    sub = df[df["_accion"] == accion].copy()

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

    # Resumen de rotación dentro del segmento
    if accion == "REPROGRAMAR":
        rot_counts = sub.groupby("_rotacion")["Producto"].nunique()
        rot_cols = st.columns(4)
        for i, rot in enumerate(["ALTA", "MEDIA", "BAJA", "NULA"]):
            cfg_r = ROTACION_CFG[rot]
            cnt_r = rot_counts.get(rot, 0)
            with rot_cols[i]:
                st.markdown(
                    f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
                    f"border-left:3px solid {cfg_r['color']};border-radius:6px;"
                    f"padding:8px 12px;'>"
                    f"<div style='font-size:9px;letter-spacing:1.5px;text-transform:uppercase;"
                    f"color:#6B6456;margin-bottom:3px;'>{cfg_r['icon']} {cfg_r['label']}</div>"
                    f"<div style='font-family:Bebas Neue,sans-serif;font-size:1.4rem;"
                    f"color:{cfg_r['color']};line-height:1;'>{cnt_r}</div>"
                    f"</div>",
                    unsafe_allow_html=True,
                )

    fb1, fb2 = st.columns([3, 2])
    with fb1:
        buscar = st.text_input("Buscar", placeholder="Buscar producto...", label_visibility="collapsed",
                               key=f"buscar_{accion}")
    with fb2:
        tipos_disp = sorted(sub["Tipo"].dropna().unique().tolist())
        tipo_sel   = st.selectbox("Categoría", ["Todas"] + tipos_disp,
                                  label_visibility="collapsed", key=f"tipo_{accion}")

    loc_cols  = [c for c in sub.columns if c.startswith("Stock_")]
    loc_names = [c.replace("Stock_", "") for c in loc_cols]
    sel_loc   = "Total"
    if loc_cols:
        sel_loc = st.selectbox("📍 Ver stock de", ["Total"] + loc_names, key=f"loc_{accion}")

    if tipo_sel != "Todas":
        sub = sub[sub["Tipo"] == tipo_sel]
    if buscar:
        sub = sub[sub["Producto"].str.contains(buscar, case=False, na=False)]

    if sub.empty:
        st.info("Sin resultados.")
        return

    mostrar_form = accion in ("REPROGRAMAR", "OK")

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
            # Mostrar rotación del producto
            rot_prod  = gp["_rotacion"].mode()[0] if not gp.empty else "NULA"
            cfg_r     = ROTACION_CFG[rot_prod]
            bs_tag    = " · ⭐ BS" if es_bs else ""

            st.markdown(
                f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
                f"border-left:3px solid {color};"
                "border-radius:8px 8px 0 0;padding:11px 14px;"
                "display:flex;align-items:center;gap:10px;'>"
                f"<div style='font-weight:600;font-size:14px;flex:1;'>{prod.upper()}</div>"
                f"<div style='font-size:11px;padding:2px 8px;border-radius:10px;"
                f"background:{cfg_r['color']}22;color:{cfg_r['color']};'>"
                f"{cfg_r['icon']} {cfg_r['label']}</div>"
                f"<div style='font-size:11px;color:#6B6456;'>{n} talla{'s' if n > 1 else ''}{bs_tag}</div>"
                "</div>"
                f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
                f"border-left:3px solid {color};"
                "display:grid;grid-template-columns:2fr 1fr 1fr 1.2fr 1fr 1fr;"
                "gap:8px;padding:5px 14px;"
                "font-size:9px;color:#6B6456;letter-spacing:1.5px;text-transform:uppercase;"
                "font-family:DM Mono,monospace;'>"
                "<div>VARIANTE</div><div>STOCK</div><div>DÍAS INV.</div>"
                "<div>VENTAS 60D</div><div>ROTACIÓN</div><div>SUGERIDO</div>"
                "</div>",
                unsafe_allow_html=True,
            )

            for _, row in gp.iterrows():
                stock_v  = int(row.get(f"Stock_{sel_loc}", row["Stock"]) if sel_loc != "Total" else row["Stock"])
                dias_n   = float(row["DiasInv_n"])
                dias_str = str(int(dias_n)) if dias_n < 9999 else "∞"
                sug, _   = sugerir_cantidad(row["Stock"], row["Ventas60d"], dias_n, accion)
                rot_var  = row["_rotacion"]
                cfg_rv   = ROTACION_CFG[rot_var]

                st.markdown(
                    f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
                    f"border-left:3px solid {color};"
                    "display:grid;grid-template-columns:2fr 1fr 1fr 1.2fr 1fr 1fr;"
                    "gap:8px;padding:8px 14px;border-top:1px solid #D4CFC4;"
                    "align-items:center;font-size:13px;'>"
                    f"<div style='font-weight:500;'>{row['Variante']}</div>"
                    f"<div style='font-family:DM Mono,monospace;color:#6B6456;font-size:12px;'>{stock_v} u</div>"
                    f"<div style='font-family:Bebas Neue,sans-serif;font-size:22px;line-height:1;"
                    f"color:{color};'>{dias_str}</div>"
                    f"<div style='font-size:12px;color:#6B6456;'>{int(row['Ventas60d'])} u</div>"
                    f"<div style='font-size:10px;padding:2px 6px;border-radius:8px;"
                    f"background:{cfg_rv['color']}22;color:{cfg_rv['color']};white-space:nowrap;'>"
                    f"{cfg_rv['icon']} {cfg_rv['label']}</div>"
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
                    float(gp["DiasInv_n"].min()), accion,
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


# ─── MÓDULOS 2-5: VENTAS, ROTACIÓN, TENDENCIAS, SKUs ─────────────────────────
# (Sin cambios funcionales — solo se actualizaron referencias de _estado → _accion)

def vista_ventas(token):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:16px;'>VENTAS</div>",
        unsafe_allow_html=True,
    )

    hoy = datetime.now().date()
    col_d, col_h, col_c = st.columns([2, 2, 2])
    with col_d:
        fecha_desde = st.date_input("Desde", value=hoy - timedelta(days=30), max_value=hoy, key="ventas_desde")
    with col_h:
        fecha_hasta = st.date_input("Hasta", value=hoy, max_value=hoy, key="ventas_hasta")
    with col_c:
        CANALES_OPCIONES = ["Todos los canales", "Online Store", "Point of Sale", "Draft Orders"]
        sel_canal = st.selectbox("Canal", CANALES_OPCIONES, key="ventas_canal")

    if fecha_desde > fecha_hasta:
        st.error("La fecha de inicio debe ser anterior a la fecha final.")
        return

    sel_rango = f"{fecha_desde.strftime('%d/%m/%Y')} → {fecha_hasta.strftime('%d/%m/%Y')}"

    with st.spinner("Cargando ventas..."):
        df_v = cargar_ventas_rango(token, fecha_desde, fecha_hasta)

    if df_v.empty:
        st.info("Sin ventas en el período.")
        return

    if "canal" not in df_v.columns:
        df_v["canal"] = "Online Store"

    df_v["fecha"] = pd.to_datetime(df_v["fecha"])

    CANALES_INFO = {
        "Online Store":  {"color": "#2D6A4F", "icon": "🌐"},
        "Point of Sale": {"color": "#4488FF", "icon": "🏪"},
        "Draft Orders":  {"color": "#FF9500", "icon": "📋"},
    }
    cols_c = st.columns(3)
    for i, (canal_name, info) in enumerate(CANALES_INFO.items()):
        sub_c   = df_v[df_v["canal"] == canal_name]
        tot_c   = sub_c["total"].sum()
        uni_c   = int(sub_c["cantidad"].sum())
        activo  = sel_canal == canal_name or sel_canal == "Todos los canales"
        opacity = "1" if activo else "0.4"
        with cols_c[i]:
            st.markdown(
                f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
                f"border-left:3px solid {info['color']};border-radius:6px;"
                f"padding:10px 14px;opacity:{opacity};'>"
                f"<div style='font-size:9px;letter-spacing:1.5px;text-transform:uppercase;"
                f"color:#6B6456;margin-bottom:4px;'>{info['icon']} {canal_name}</div>"
                f"<div style='font-family:Bebas Neue,sans-serif;font-size:1.5rem;"
                f"color:{info['color']};line-height:1;'>{fmt_pesos(tot_c)}</div>"
                f"<div style='font-size:10px;color:#6B6456;margin-top:3px;'>{uni_c:,} unidades</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

    st.markdown("<hr style='border-color:#D4CFC4;margin:16px 0;'>", unsafe_allow_html=True)

    df_view = df_v.copy() if sel_canal == "Todos los canales" else df_v[df_v["canal"] == sel_canal].copy()

    if df_view.empty:
        st.info(f"Sin ventas para '{sel_canal}' en este período.")
        return

    tot   = df_view["total"].sum()
    unids = int(df_view["cantidad"].sum())

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Ventas brutas",      fmt_pesos(tot))
    with c2: st.metric("Unidades vendidas",  f"{unids:,}")
    with c3: st.metric("Ticket promedio",    fmt_pesos(tot / unids) if unids else "—")

    st.markdown(
        "<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-left:3px solid #FFB800;"
        "border-radius:6px;padding:10px 14px;margin:8px 0 16px 0;font-size:12px;color:#6B6456;'>"
        "⚠️ <b>Ventas brutas</b> — precio original × unidades, sin descontar descuentos ni devoluciones. "
        "El desfase respecto a Shopify es típicamente <b>5–15%</b>."
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin-bottom:6px;'>EVOLUCIÓN DIARIA</div>",
        unsafe_allow_html=True,
    )
    evol = df_view.groupby("fecha").agg(total=("total", "sum")).reset_index()
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

    _seccion("PARETO 80 / 20", f"Productos que generan el 80% de las ventas brutas · {sel_rango}")

    pareto = (
        df_view.groupby("producto")
        .agg(total=("total", "sum"), unidades=("cantidad", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
        .reset_index(drop=True)
    )
    pareto["acum_pct"] = pareto["total"].cumsum() / pareto["total"].sum() * 100
    pareto["rank"]     = range(1, len(pareto) + 1)
    pareto["prod_pct"] = pareto["rank"] / len(pareto) * 100

    corte_idx   = int((pareto["acum_pct"] <= 80).sum())
    n_total     = len(pareto)
    n_vitales   = max(corte_idx, 1)
    n_triviales = n_total - n_vitales
    pct_prods   = round(n_vitales / n_total * 100, 1)
    rev_vitales = pareto.iloc[:n_vitales]["total"].sum()

    c1, c2, c3, c4 = st.columns(4)
    with c1: st.metric("Productos vitales (80%)", n_vitales)
    with c2: st.metric("% del catálogo",          f"{pct_prods}%")
    with c3: st.metric("Productos triviales",      n_triviales)
    with c4: st.metric("Revenue vitales",          fmt_pesos(rev_vitales))

    colores_bar = ["#2D6A4F" if i < n_vitales else "#D4CFC4" for i in range(len(pareto))]
    fig_pareto = go.Figure()
    fig_pareto.add_trace(go.Bar(
        x=list(range(len(pareto))), y=pareto["total"].tolist(),
        marker_color=colores_bar, marker_opacity=0.85, name="Revenue",
        hovertemplate="<b>%{customdata}</b><br>$%{y:,.0f}<extra></extra>",
        customdata=pareto["producto"].tolist(), yaxis="y1",
    ))
    fig_pareto.add_trace(go.Scatter(
        x=list(range(len(pareto))), y=pareto["acum_pct"].tolist(),
        mode="lines", line=dict(color="#FF9500", width=2),
        name="Acumulado %", hovertemplate="%{y:.1f}%<extra></extra>", yaxis="y2",
    ))
    fig_pareto.add_hline(y=80, line_dash="dot", line_color="#FF3B30", line_width=1.5,
                         annotation_text="80%", annotation_font_size=10,
                         annotation_font_color="#FF3B30", yref="y2")
    if n_vitales < len(pareto):
        fig_pareto.add_vline(x=n_vitales - 0.5, line_dash="dot", line_color="#FF3B30", line_width=1)
    fig_pareto.update_layout(
        **PLOT_BASE, height=340, margin=dict(t=20, b=60, l=70, r=60),
        showlegend=False, bargap=0.15,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=True, gridcolor="#D4CFC4", tickprefix="$", tickformat=",.0f",
                   tickfont=dict(size=9), title=None),
        yaxis2=dict(overlaying="y", side="right", range=[0, 105], ticksuffix="%",
                    tickfont=dict(size=9), showgrid=False, title=None),
        annotations=[
            dict(x=n_vitales/2, y=-0.18, xref="x", yref="paper",
                 text=f"◀ {n_vitales} vitales · {pct_prods}% del catálogo",
                 showarrow=False, font=dict(size=10, color="#2D6A4F"), xanchor="center"),
            dict(x=(n_vitales+n_total)/2, y=-0.18, xref="x", yref="paper",
                 text=f"{n_triviales} triviales ▶",
                 showarrow=False, font=dict(size=10, color="#B8B0A4"), xanchor="center"),
        ],
    )
    st.plotly_chart(fig_pareto, use_container_width=True, config={"displayModeBar": False})

    col_v, col_t = st.columns(2)
    with col_v:
        st.markdown(
            f"<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            f"letter-spacing:2px;color:#2D6A4F;margin-bottom:8px;'>"
            f"VITALES — {n_vitales} productos · 80% del revenue</div>",
            unsafe_allow_html=True,
        )
        vitales = pareto.iloc[:n_vitales].copy()
        max_v   = vitales["total"].max() or 1
        v_html  = "".join(
            f"<tr><td style='padding:5px 10px 5px 0;font-size:11px;color:#6B6456;"
            f"font-family:DM Mono,monospace;'>{int(r['rank'])}</td>"
            f"<td style='padding:5px 10px;font-size:12px;font-weight:500;color:#1A1A14;'>{r['producto'][:34]}</td>"
            f"<td style='padding:5px 0;width:30%;'><div style='background:#D4CFC4;border-radius:2px;height:12px;'>"
            f"<div style='background:#2D6A4F;width:{int(r['total']/max_v*100)}%;height:12px;border-radius:2px;opacity:0.85;'>"
            f"</div></div></td>"
            f"<td style='padding:5px 0 5px 8px;font-family:DM Mono,monospace;font-size:11px;"
            f"color:#2D6A4F;text-align:right;font-weight:600;'>{fmt_pesos(r['total'])}</td>"
            f"<td style='padding:5px 0 5px 6px;font-family:DM Mono,monospace;font-size:10px;"
            f"color:#B8B0A4;text-align:right;'>{r['acum_pct']:.1f}%</td></tr>"
            for _, r in vitales.iterrows()
        )
        st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{v_html}</tbody></table>",
                    unsafe_allow_html=True)

    with col_t:
        st.markdown(
            f"<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            f"letter-spacing:2px;color:#B8B0A4;margin-bottom:8px;'>"
            f"TRIVIALES — {n_triviales} productos</div>",
            unsafe_allow_html=True,
        )
        triviales = pareto.iloc[n_vitales:].copy()
        if triviales.empty:
            st.markdown("<div style='font-size:12px;color:#B8B0A4;padding:20px 0;'>Sin productos.</div>",
                        unsafe_allow_html=True)
        else:
            max_t  = triviales["total"].max() or 1
            t_html = "".join(
                f"<tr><td style='padding:5px 10px 5px 0;font-size:11px;color:#B8B0A4;"
                f"font-family:DM Mono,monospace;'>{int(r['rank'])}</td>"
                f"<td style='padding:5px 10px;font-size:12px;color:#6B6456;'>{r['producto'][:34]}</td>"
                f"<td style='padding:5px 0;width:30%;'><div style='background:#D4CFC4;border-radius:2px;height:12px;'>"
                f"<div style='background:#B8B0A4;width:{int(r['total']/max_t*100)}%;height:12px;"
                f"border-radius:2px;opacity:0.6;'></div></div></td>"
                f"<td style='padding:5px 0 5px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#6B6456;text-align:right;'>{fmt_pesos(r['total'])}</td>"
                f"<td style='padding:5px 0 5px 6px;font-family:DM Mono,monospace;font-size:10px;"
                f"color:#B8B0A4;text-align:right;'>{r['acum_pct']:.1f}%</td></tr>"
                for _, r in triviales.iterrows()
            )
            st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{t_html}</tbody></table>",
                        unsafe_allow_html=True)

    _seccion("TOP PRODUCTOS", f"Por valor de venta · {sel_rango}")
    tp = (
        df_view.groupby("producto")
        .agg(total=("total", "sum"), unidades=("cantidad", "sum"))
        .reset_index()
        .sort_values("total", ascending=False)
        .head(15)
    )
    max_tp  = tp["total"].max() if len(tp) else 1
    tp_html = "".join(
        f"<tr><td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;color:#1A1A14;'>{row.producto}</td>"
        f"<td style='padding:7px 8px;width:45%;'><div style='background:#D4CFC4;border-radius:3px;height:16px;'>"
        f"<div style='background:#2D6A4F;width:{int(row.total/max_tp*100)}%;height:16px;border-radius:3px;opacity:0.85;'>"
        f"</div></div></td>"
        f"<td style='padding:7px 4px;font-family:DM Mono,monospace;font-size:12px;color:#2D6A4F;"
        f"text-align:right;font-weight:600;'>{fmt_pesos(row.total)}</td>"
        f"<td style='padding:7px 0 7px 12px;font-family:DM Mono,monospace;font-size:12px;"
        f"color:#6B6456;text-align:right;'>{int(row.unidades)} u</td></tr>"
        for row in tp.itertuples()
    )
    st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{tp_html}</tbody></table>",
                unsafe_allow_html=True)

    _seccion("DETALLE POR SKU", "Variantes ordenadas por unidades vendidas")
    det = (
        df_view.groupby(["producto", "sku", "variante"])
        .agg(unidades=("cantidad", "sum"), total=("total", "sum"))
        .reset_index()
        .sort_values(["producto", "unidades"], ascending=[True, False])
    )
    for prod, grupo in det.groupby("producto", sort=False):
        total_prod = grupo["total"].sum()
        unids_prod = int(grupo["unidades"].sum())
        st.markdown(
            f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;"
            f"border-left:3px solid #2D6A4F;border-radius:6px 6px 0 0;"
            f"padding:10px 14px;display:flex;align-items:center;justify-content:space-between;'>"
            f"<div style='font-weight:600;font-size:13px;color:#1A1A14;'>{prod}</div>"
            f"<div style='font-family:DM Mono,monospace;font-size:12px;color:#2D6A4F;'>"
            f"{fmt_pesos(total_prod)} · {unids_prod} u</div></div>",
            unsafe_allow_html=True,
        )
        rows_sku = "".join(
            f"<tr style='border-top:1px solid #D4CFC4;'>"
            f"<td style='padding:6px 14px;font-size:12px;color:#6B6456;font-family:DM Mono,monospace;'>{r.sku}</td>"
            f"<td style='padding:6px 14px;font-size:12px;color:#1A1A14;'>{r.variante}</td>"
            f"<td style='padding:6px 14px;font-family:DM Mono,monospace;font-size:12px;text-align:right;'>{int(r.unidades)} u</td>"
            f"<td style='padding:6px 14px;font-family:DM Mono,monospace;font-size:12px;color:#2D6A4F;"
            f"text-align:right;font-weight:600;'>{fmt_pesos(r.total)}</td></tr>"
            for r in grupo.itertuples()
        )
        st.markdown(
            f"<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-top:none;"
            f"border-left:3px solid #2D6A4F;border-radius:0 0 6px 6px;overflow:hidden;'>"
            f"<table style='width:100%;border-collapse:collapse;'><tbody>{rows_sku}</tbody></table>"
            f"</div><div style='height:6px;'></div>",
            unsafe_allow_html=True,
        )


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

    liq = df[df["_accion"] == "LIQUIDAR"].copy()
    rep = df[df["_accion"].isin(["REPROGRAMAR"])].copy()

    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:14px;"
        "letter-spacing:2px;color:#FF9500;margin-bottom:8px;'>PASO 1 — CAPITAL INMOVILIZADO (LIQUIDAR)</div>",
        unsafe_allow_html=True,
    )

    desc_pct = st.slider("Descuento de liquidación (%)", 10, 60, 30, 5, key="desc_liq")
    factor   = 1 - desc_pct / 100

    capital_total = 0.0

    if liq.empty:
        st.info("No hay productos en LIQUIDAR actualmente.")
    else:
        liq_ag = liq.groupby("Producto").agg(
            stock=("Stock", "sum"), precio=("Precio Venta", "mean"),
            costo=("Costo", "mean"), ventas=("Ventas60d", "sum"),
        ).reset_index()
        liq_ag = liq_ag[liq_ag["stock"] > 0].copy()
        liq_ag["precio_liq"]  = liq_ag["precio"] * factor
        liq_ag["valor_costo"] = liq_ag["stock"] * liq_ag["costo"]
        liq_ag["capital_liq"] = liq_ag["stock"] * liq_ag["precio_liq"]
        capital_total = liq_ag["capital_liq"].sum()

        liq_plot = liq_ag.sort_values("capital_liq", ascending=False).head(15)
        max_liq  = liq_plot["capital_liq"].max() or 1
        liq_html = "".join(
            f"<tr><td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;color:#1A1A14;'>{r['Producto']}</td>"
            f"<td style='padding:7px 8px;width:40%;'><div style='background:#D4CFC4;border-radius:3px;height:16px;'>"
            f"<div style='background:#FF9500;width:{min(100,int(r['capital_liq']/max_liq*100))}%;height:16px;"
            f"border-radius:3px;opacity:0.85;'></div></div></td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#FF9500;text-align:right;font-weight:600;'>{fmt_pesos(r['capital_liq'])}</td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#6B6456;text-align:right;'>{int(r['stock'])} u</td></tr>"
            for _, r in liq_plot.iterrows()
        )
        st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{liq_html}</tbody></table>",
                    unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("Productos a liquidar", len(liq_ag))
        with c2: st.metric("Unidades totales",     int(liq_ag["stock"].sum()))
        with c3: st.metric(f"Capital estimado ({desc_pct}% desc.)", fmt_pesos(capital_total))

    st.markdown("<hr style='border-color:#D4CFC4;margin:24px 0;'>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:14px;"
        "letter-spacing:2px;color:#2D6A4F;margin-bottom:8px;'>PASO 2 — ¿QUÉ REPONGO CON ESE CAPITAL?</div>",
        unsafe_allow_html=True,
    )

    presupuesto = st.number_input(
        "Presupuesto disponible ($COP)",
        min_value=0, value=int(capital_total), step=100_000, key="presupuesto_rot",
    )

    if rep.empty:
        st.info("No hay productos en REPROGRAMAR.")
        return

    rep_ag = rep.groupby("Producto").agg(
        costo=("Costo", "mean"), ventas=("Ventas60d", "sum"),
        stock=("Stock", "sum"), dias=("DiasInv_n", "min"),
        accion=("_accion", "first"), rotacion=("_rotacion", "first"),
    ).reset_index()
    rep_ag = rep_ag[rep_ag["costo"] > 0].sort_values("ventas", ascending=False)
    rep_ag["sug_unids"] = rep_ag.apply(
        lambda r: sugerir_cantidad(r["stock"], r["ventas"], r["dias"], r["accion"])[0], axis=1
    )
    rep_ag["costo_sug"]      = rep_ag["sug_unids"] * rep_ag["costo"]
    presupuesto_rest         = float(presupuesto)
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
        max_sug  = rep_plot["costo_sug"].max() or 1
        rep_html = "".join(
            f"<tr><td style='padding:7px 12px 7px 0;font-size:13px;font-weight:500;color:#1A1A14;'>{r['Producto']}</td>"
            f"<td style='padding:7px 8px;width:40%;position:relative;'>"
            f"<div style='background:#D4CFC4;border-radius:3px;height:18px;position:relative;'>"
            f"<div style='background:#D4CFC4;width:{min(100,int(r['costo_sug']/max_sug*100))}%;height:18px;"
            f"border-radius:3px;position:absolute;top:0;left:0;opacity:0.5;'></div>"
            f"<div style='background:#2D6A4F;width:{min(100,int(r['costo_real']/max_sug*100))}%;height:18px;"
            f"border-radius:3px;position:absolute;top:0;left:0;opacity:0.9;'></div>"
            f"</div></td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#2D6A4F;text-align:right;font-weight:600;'>{int(r['unids_posibles'])} u</td>"
            f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:12px;"
            f"color:#1A1A14;text-align:right;'>{fmt_pesos(r['costo_real'])}</td></tr>"
            for _, r in rep_plot.iterrows()
        )
        st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{rep_html}</tbody></table>",
                    unsafe_allow_html=True)

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
            + ("..." if len(rep_sin) > 5 else "") + "</div>",
            unsafe_allow_html=True,
        )


def vista_tendencias(token):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:8px;'>TENDENCIAS</div>",
        unsafe_allow_html=True,
    )
    st.markdown(
        "<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-left:3px solid #FFB800;"
        "border-radius:6px;padding:8px 14px;margin-bottom:16px;font-size:11px;color:#6B6456;'>"
        "⚠️ Las tendencias se basan en <b>unidades vendidas</b> — exactas. "
        "Los valores de revenue son ventas brutas con desfase estimado de <b>5–15%</b> vs Shopify."
        "</div>",
        unsafe_allow_html=True,
    )

    with st.spinner("Cargando 90 días de ventas..."):
        hoy_t   = datetime.now().date()
        desde_t = hoy_t - timedelta(days=90)
        df_t    = cargar_ventas_rango(token, desde_t, hoy_t)

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
    comp = comp[(comp["reciente"] >= 3) & (comp["anterior"] >= 3)].copy()
    comp["delta"] = comp["reciente"] - comp["anterior"]
    comp["pct"]   = (comp["delta"] / comp["anterior"] * 100).round(0)
    comp = comp.reset_index()
    comp.columns = ["Producto", "Últimos 30d", "30d ant.", "Δ u", "Δ %"]
    comp = comp.sort_values("Δ %", ascending=False)

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
                f"<tr><td style='padding:7px 12px 7px 0;font-size:12px;font-weight:500;color:#1A1A14;'>{r['Producto']}</td>"
                f"<td style='padding:7px 8px;width:35%;'><div style='background:#D4CFC4;border-radius:3px;height:14px;'>"
                f"<div style='background:#2D6A4F;width:{min(100,int(r['Δ u']/max_crec*100))}%;height:14px;"
                f"border-radius:3px;opacity:0.85;'></div></div></td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#2D6A4F;text-align:right;font-weight:600;'>+{int(r['Últimos 30d'])} u</td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#6B6456;text-align:right;'>{r['Δ %']:+.0f}%</td></tr>"
                for _, r in top_crec.iterrows()
            )
            st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{crec_html}</tbody></table>",
                        unsafe_allow_html=True)

    with col_r:
        st.markdown(
            "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
            "letter-spacing:2px;color:#FF3B30;margin-bottom:10px;'>📉 DESACELERANDO</div>",
            unsafe_allow_html=True,
        )
        if top_dec.empty:
            st.info("Sin productos con tendencia decreciente significativa.")
        else:
            max_dec  = top_dec["Δ u"].abs().quantile(0.85) or top_dec["Δ u"].abs().max() or 1
            dec_html = "".join(
                f"<tr><td style='padding:7px 12px 7px 0;font-size:12px;font-weight:500;color:#1A1A14;'>{r['Producto']}</td>"
                f"<td style='padding:7px 8px;width:35%;'><div style='background:#D4CFC4;border-radius:3px;height:14px;'>"
                f"<div style='background:#FF3B30;width:{min(100,int(abs(r['Δ u'])/max_dec*100))}%;height:14px;"
                f"border-radius:3px;opacity:0.75;'></div></div></td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#FF3B30;text-align:right;font-weight:600;'>{int(r['Últimos 30d'])} u</td>"
                f"<td style='padding:7px 0 7px 8px;font-family:DM Mono,monospace;font-size:11px;"
                f"color:#6B6456;text-align:right;'>{r['Δ %']:+.0f}%</td></tr>"
                for _, r in top_dec.iterrows()
            )
            st.markdown(f"<table style='width:100%;border-collapse:collapse;'><tbody>{dec_html}</tbody></table>",
                        unsafe_allow_html=True)

    st.markdown(
        "<div style='font-size:10px;color:#B8B0A4;margin-bottom:20px;'>"
        "Solo se muestran productos con ≥ 3 unidades vendidas en ambos períodos."
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:13px;"
        "letter-spacing:2px;color:#6B6456;margin:16px 0 6px 0;'>EVOLUCIÓN SEMANAL — DRILL DOWN</div>",
        unsafe_allow_html=True,
    )
    top5       = df_t[df_t["fecha"] >= corte].groupby("producto")["cantidad"].sum().nlargest(5).index.tolist()
    prods_disp = sorted(df_t["producto"].unique().tolist())
    sel_prods  = st.multiselect("Seleccionar productos", prods_disp, default=top5[:3], key="sel_tend")

    if sel_prods:
        df_sel = df_t[df_t["producto"].isin(sel_prods)].copy()
        df_sel["semana"] = df_sel["fecha"].dt.to_period("W").dt.start_time
        evol    = df_sel.groupby(["semana", "producto"])["cantidad"].sum().reset_index()
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
        fig_ev.add_vline(
            x=corte.timestamp() * 1000,
            line=dict(color="#B8B0A4", width=1, dash="dot"),
            annotation_text="hace 30d", annotation_font_size=9,
        )
        fig_ev.update_layout(
            **PLOT_BASE, height=320,
            margin=dict(t=20, b=30, l=50, r=20),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, font=dict(size=10)),
            xaxis=dict(showgrid=False, tickformat="%d %b"),
            yaxis=dict(showgrid=True, gridcolor="#D4CFC4", tickfont=dict(size=9), title="Unidades"),
        )
        st.plotly_chart(fig_ev, use_container_width=True, config={"displayModeBar": False})


# ─── MÓDULO 5: GESTIÓN DE SKUs ────────────────────────────────────────────────

def _sku_format(prefix, n):
    if n >= 1000:
        return f"{prefix}{n:04d}"
    return f"{prefix}{n:03d}"


def _ean13_generate(seed_str):
    random.seed(seed_str)
    digits = [random.randint(0, 9) for _ in range(12)]
    check  = (10 - sum(d * (1 if i % 2 == 0 else 3) for i, d in enumerate(digits)) % 10) % 10
    return "".join(map(str, digits)) + str(check)


def _sku_fetch_all_products(token):
    shop   = st.secrets["TIENDA_URL"]
    url    = f"https://{shop}/admin/api/{API_VERSION}/products.json"
    params = {"limit": 250, "fields": "id,title,product_type,variants", "status": "active"}
    products = []
    hdrs   = _headers(token)
    while url:
        r = requests.get(url, headers=hdrs, params=params, timeout=30)
        r.raise_for_status()
        products.extend(r.json().get("products", []))
        link   = r.headers.get("Link", "")
        url    = None
        params = {}
        for part in link.split(","):
            if 'rel="next"' in part:
                url = part.split(";")[0].strip().strip("<>")
    return products


def _sku_compute_counters(products):
    counters = defaultdict(int)
    pattern  = re.compile(r"^([A-Z]{3})(\d{3,4})$")
    for p in products:
        for v in p.get("variants", []):
            sku = (v.get("sku") or "").strip().upper()
            m   = pattern.match(sku)
            if m:
                prefix, num = m.group(1), int(m.group(2))
                counters[prefix] = max(counters[prefix], num)
    return dict(counters)


def _sku_collect_unassigned(products):
    result = []
    for p in products:
        ptype  = p.get("product_type", "").strip()
        prefix = PREFIX_MAP.get(ptype)
        for v in p.get("variants", []):
            sku     = (v.get("sku") or "").strip()
            barcode = (v.get("barcode") or "").strip()
            if not sku:
                result.append({
                    "product_id":      str(p["id"]),
                    "product_title":   p["title"],
                    "product_type":    ptype,
                    "prefix":          prefix,
                    "variant_id":      str(v["id"]),
                    "variant_title":   v.get("title", ""),
                    "current_barcode": barcode,
                })
    return result


def _sku_assign(unassigned, counters):
    local_counters = dict(counters)
    assigned       = []
    seen_barcodes  = set()
    for row in unassigned:
        prefix = row["prefix"]
        if not prefix:
            assigned.append({**row, "new_sku": None, "new_barcode": None,
                              "error": f"Tipo sin prefijo configurado: '{row['product_type']}'"})
            continue
        local_counters[prefix] = local_counters.get(prefix, 0) + 1
        n       = local_counters[prefix]
        new_sku = _sku_format(prefix, n)
        if row["current_barcode"]:
            new_barcode = row["current_barcode"]
        else:
            candidate = _ean13_generate(new_sku)
            attempts  = 0
            while candidate in seen_barcodes and attempts < 20:
                candidate = _ean13_generate(new_sku + str(attempts))
                attempts += 1
            new_barcode = candidate
            seen_barcodes.add(new_barcode)
        assigned.append({**row, "new_sku": new_sku, "new_barcode": new_barcode, "error": None})
    return assigned


def _sku_push_variant(token, variant_id, new_sku, new_barcode, has_barcode):
    shop    = st.secrets["TIENDA_URL"]
    url     = f"https://{shop}/admin/api/{API_VERSION}/variants/{variant_id}.json"
    payload = {"variant": {"id": int(variant_id), "sku": new_sku}}
    if not has_barcode:
        payload["variant"]["barcode"] = new_barcode
    r = requests.put(url, headers=_headers(token), json=payload, timeout=15)
    return r.status_code, r.json()


def vista_sku_manager(token):
    st.markdown(
        "<div style='font-family:Bebas Neue,sans-serif;font-size:26px;"
        "letter-spacing:3px;color:#1A1A14;margin-bottom:4px;'>GESTIÓN DE SKUs</div>"
        "<div style='font-size:11px;color:#6B6456;letter-spacing:1px;"
        "text-transform:uppercase;margin-bottom:20px;'>"
        "Genera y sube SKU + barcode automáticamente · Solo variantes sin SKU asignado</div>",
        unsafe_allow_html=True,
    )

    if st.button("🔍 ESCANEAR PRODUCTOS SIN SKU", key="btn_scan_sku"):
        with st.spinner("Leyendo todos los productos de Shopify..."):
            try:
                products = _sku_fetch_all_products(token)
                st.session_state["_sku_products"]   = products
                st.session_state["_sku_counters"]   = _sku_compute_counters(products)
                st.session_state["_sku_unassigned"] = _sku_collect_unassigned(products)
                st.session_state["_sku_assigned"]   = None
            except Exception as e:
                st.error(f"Error al leer Shopify: {e}")
                return

    products   = st.session_state.get("_sku_products")
    counters   = st.session_state.get("_sku_counters")
    unassigned = st.session_state.get("_sku_unassigned")

    if products is None:
        st.markdown(
            "<div style='background:#EDEAE0;border:1px solid #D4CFC4;border-left:3px solid #4488FF;"
            "border-radius:8px;padding:16px 20px;font-size:13px;color:#6B6456;'>"
            "Presiona el botón para escanear todos los productos de Shopify.</div>",
            unsafe_allow_html=True,
        )
        return

    total_variants = sum(len(p.get("variants", [])) for p in products)
    c1, c2, c3 = st.columns(3)
    with c1: st.metric("Productos en Shopify", len(products))
    with c2: st.metric("Variantes totales",    total_variants)
    with c3: st.metric("Variantes sin SKU",    len(unassigned))

    if not unassigned:
        st.success("✅ Todos los productos y variantes ya tienen SKU asignado.")
        return

    if st.button("⚡ GENERAR PREVIEW DE SKUs", key="btn_preview_sku"):
        assigned = _sku_assign(unassigned, counters)
        st.session_state["_sku_assigned"] = assigned

    assigned = st.session_state.get("_sku_assigned")
    if assigned is None:
        return

    ok     = [a for a in assigned if not a["error"]]
    errors = [a for a in assigned if a["error"]]

    if errors:
        with st.expander(f"⚠️ {len(errors)} variantes sin prefijo configurado", expanded=False):
            for e in errors:
                st.markdown(f"<div style='font-size:12px;color:#FF9500;padding:3px 0;'>"
                            f"<b>{e['product_title']}</b> · {e['variant_title']} — {e['error']}</div>",
                            unsafe_allow_html=True)

    if ok:
        col_btn1, col_btn2, _ = st.columns([2, 2, 4])
        with col_btn1:
            confirmar = st.button("✅ CONFIRMAR Y SUBIR A SHOPIFY", key="btn_confirm_sku")
        with col_btn2:
            if st.button("🔄 VOLVER A ESCANEAR", key="btn_rescan_sku"):
                for k in ["_sku_products", "_sku_counters", "_sku_unassigned", "_sku_assigned"]:
                    st.session_state.pop(k, None)
                st.rerun()

        if confirmar:
            progress = st.progress(0, text="Subiendo SKUs a Shopify...")
            res_ok, res_err, res_detalle = 0, 0, []
            for i, a in enumerate(ok):
                try:
                    status, _ = _sku_push_variant(
                        token, a["variant_id"], a["new_sku"], a["new_barcode"],
                        bool(a["current_barcode"])
                    )
                    if status == 200:
                        res_ok += 1
                    else:
                        res_err += 1
                        res_detalle.append(f"{a['product_title']} / {a['variant_title']}: HTTP {status}")
                except Exception as exc:
                    res_err += 1
                    res_detalle.append(f"{a['product_title']} / {a['variant_title']}: {exc}")
                progress.progress((i + 1) / len(ok), text=f"Subiendo {i+1}/{len(ok)}...")
            progress.empty()
            if res_ok > 0:
                st.success(f"✅ {res_ok} variantes actualizadas correctamente en Shopify.")
            if res_err > 0:
                st.error(f"❌ {res_err} errores al subir.")
            for k in ["_sku_products", "_sku_counters", "_sku_unassigned", "_sku_assigned"]:
                st.session_state.pop(k, None)
            st.cache_data.clear()
            st.rerun()


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

    # Conteos por acción para el sidebar
    conteos = {}
    if not df.empty:
        for accion in ACCION_CFG:
            conteos[accion] = int(df[df["_accion"] == accion]["Producto"].nunique())

    if "vista" not in st.session_state:
        st.session_state.vista = "DASHBOARD"

    render_sidebar(conteos)
    render_guia_flotante()

    vista = st.session_state.get("vista", "DASHBOARD")

    if vista == "DASHBOARD":
        vista_dashboard(df, locations, token)
    elif vista == "VENTAS":
        vista_ventas(token)
    elif vista == "ROTACION":
        vista_rotacion(df)
    elif vista == "TENDENCIAS":
        vista_tendencias(token)
    elif vista == "SKU_MGR":
        vista_sku_manager(token)
    elif vista in ACCION_CFG:
        vista_inventario(df, vista, locations)

    st.markdown(
        f"<div style='font-size:10px;color:#D4CFC4;text-align:right;margin-top:40px;'>"
        f"LÍNEA VIVA v9 · TÉRRET · {datetime.now().strftime('%d.%m.%Y %H:%M')}</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
