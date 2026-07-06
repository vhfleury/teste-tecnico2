"""Streamlit visibility layer over the analytics (gold) Delta tables.

Reads the gold Delta tables straight from the lakehouse with delta-rs (the
``deltalake`` package) — no Spark, no JVM — so it runs locally or as the
``dashboard`` Docker Compose service. Charts are tailored to each table's real
schema (Altair). Point it at the lakehouse with the ``LAKEHOUSE_DIR`` env var
(defaults to ``<repo>/lakehouse``).
"""
from __future__ import annotations

import os

import altair as alt
import pandas as pd
import streamlit as st
from deltalake import DeltaTable

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAKEHOUSE_DIR = os.environ.get("LAKEHOUSE_DIR") or os.path.join(_REPO_ROOT, "lakehouse")
ANALYTICS_ROOT = os.path.join(LAKEHOUSE_DIR, "analytics")

PARTITION_COLUMN = "ingest_date"
ACCENT = "#2b6cb0"
POSITIVE = "#1f9d55"  # green — good / completed
NEGATIVE = "#e0533d"  # red — bad / delayed / cancelled
# Stable palette for categorical dimensions with no inherent good/bad meaning.
FALLBACK_PALETTE = ("#2b6cb0", "#e0a458", "#7c5cbf", "#4c9f70", "#c65f5f", "#3aa5a0")
# Semantic colors: a category always keeps the same color across every chart,
# and the connotation matches (completed = green, delayed/cancelled = red).
GEO_COLORS = {"em_geocerca": POSITIVE, "em_rota": ACCENT}
STATUS_COLORS = {
    "CONCLUIDA": POSITIVE,
    "EM_TRANSITO": ACCENT,
    "ATRASADA": NEGATIVE,
    "CANCELADA": "#8b2f42",
}

FACTS = {
    "viagens_enriquecidas": (
        "Fato de viagens: veículo, motorista, geocercas, duração, atraso, "
        "velocidade média e quarentena."
    ),
    "posicoes_geocercas": (
        "Posições GPS classificadas (em_geocerca / em_rota) com eventos de "
        "entrada e saída de geocerca."
    ),
}
METRICS = {
    "viagens_por_mes_status": "Contagem de viagens por mês e status.",
    "taxa_atraso_mensal": "Percentual de viagens atrasadas por mês.",
    "utilizacao_frota_mensal": "Utilização da frota por mês.",
    "top_motoristas": "Top 10 motoristas por viagens concluídas.",
    "tempo_medio_por_rota": "Duração média das viagens por rota (origem → destino).",
    "tempo_parado_geocercas": "Tempo de permanência por tipo de geocerca.",
}
TABLES = {**FACTS, **METRICS}


# --------------------------------------------------------------------------- IO
@st.cache_data(ttl=60, show_spinner=False)
def load_table(name: str) -> pd.DataFrame:
    """Read a gold Delta table (latest snapshot, all partitions) into pandas."""
    return DeltaTable(os.path.join(ANALYTICS_ROOT, name)).to_pandas()


def safe_load(name: str) -> pd.DataFrame | None:
    """Load a table, returning None when it is not materialized yet."""
    try:
        return load_table(name)
    except Exception:
        return None


@st.cache_data(ttl=60, show_spinner=False)
def all_partitions() -> list[str]:
    """Union of ``ingest_date`` values across every materialized table."""
    dates: set[str] = set()
    for name in TABLES:
        df = safe_load(name)
        if df is not None and PARTITION_COLUMN in df.columns:
            dates.update(df[PARTITION_COLUMN].dropna().astype(str).unique())
    return sorted(dates)


def get(name: str, selected: list[str]) -> pd.DataFrame | None:
    """Load a table filtered to the selected ingest_date partitions."""
    df = safe_load(name)
    if df is None or not selected or PARTITION_COLUMN not in df.columns:
        return df
    return df[df[PARTITION_COLUMN].astype(str).isin(selected)]


# ------------------------------------------------------------------- formatting
def fmt_int(value: float) -> str:
    return f"{int(value):,}".replace(",", ".")


def fmt_pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def split_valid(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split enriched trips into valid (vehicle and driver resolved) and invalid.

    Same rule the gold metrics apply: orphan trips stay in the fact table but
    are excluded from the metrics/charts.
    """
    resolved = df["motorista_nome"].notna() & df["veiculo_placa"].notna()
    return df[resolved], df[~resolved]


# ------------------------------------------------------------------------ style
def inject_css() -> None:
    st.markdown(
        """
        <style>
        .block-container {padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1400px;}
        #MainMenu, footer {visibility: hidden;}
        .kpi {background: rgba(130,130,150,0.06); border: 1px solid rgba(130,130,150,0.20);
              border-left: 3px solid #2b6cb0; border-radius: 12px; padding: 14px 16px;
              height: 100%;}
        .kpi .label {font-size: 0.72rem; text-transform: uppercase; letter-spacing: .05em;
                     opacity: .65;}
        .kpi .value {font-size: 1.7rem; font-weight: 700; line-height: 1.3;}
        .kpi .sub {font-size: 0.78rem; opacity: .6;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi(column, label: str, value: str, sub: str = "", accent: str = ACCENT) -> None:
    column.markdown(
        f"<div class='kpi' style='border-left-color:{accent}'>"
        f"<div class='label'>{label}</div>"
        f"<div class='value'>{value}</div><div class='sub'>{sub}</div></div>",
        unsafe_allow_html=True,
    )


# ----------------------------------------------------------------- Altair charts
def _style(chart: alt.Chart) -> alt.Chart:
    return (
        chart.configure_view(strokeWidth=0)
        .configure_axis(grid=True, gridOpacity=0.12, labelColor="#8a8f98", titleColor="#8a8f98")
        .configure_title(fontSize=14, anchor="start")
    )


def _render(chart: alt.Chart) -> None:
    st.altair_chart(_style(chart), use_container_width=True)


def _color(df, field, mapping):
    """Color encoding: semantic colors where the category is known, a stable
    fallback palette otherwise — so a category always keeps the same color."""
    categories = list(pd.Series(df[field]).dropna().unique())
    palette, fallback = [], 0
    for category in categories:
        if category in mapping:
            palette.append(mapping[category])
        else:
            palette.append(FALLBACK_PALETTE[fallback % len(FALLBACK_PALETTE)])
            fallback += 1
    return alt.Color(
        f"{field}:N", scale=alt.Scale(domain=categories, range=palette), title=None
    )


def _bar(df, x, y, title, *, color=None, color_mapping=None, pct=False):
    tooltip = [alt.Tooltip(f"{x}:N")]
    if color:
        tooltip.append(alt.Tooltip(f"{color}:N"))
    tooltip.append(alt.Tooltip(f"{y}:Q", format=".1%" if pct else ".2f"))
    enc = dict(
        x=alt.X(f"{x}:N", title=None, sort="ascending"),
        y=alt.Y(f"{y}:Q", title=None, axis=alt.Axis(format="%") if pct else alt.Axis()),
        tooltip=tooltip,
    )
    if color:
        enc["color"] = _color(df, color, color_mapping or {})
        enc["xOffset"] = alt.XOffset(f"{color}:N")
    return (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=3, color=ACCENT)
        .encode(**enc)
        .properties(title=title, height=300)
    )


def _line(df, x, y, title, *, pct=False, color=ACCENT):
    return (
        alt.Chart(df)
        .mark_line(point=True, color=color)
        .encode(
            x=alt.X(f"{x}:N", title=None, sort="ascending"),
            y=alt.Y(f"{y}:Q", title=None, axis=alt.Axis(format="%") if pct else alt.Axis()),
            tooltip=[alt.Tooltip(f"{x}:N"), alt.Tooltip(f"{y}:Q", format=".1%" if pct else ".2f")],
        )
        .properties(title=title, height=300)
    )


def _hbar(df, category, value, title):
    height = max(220, 34 * len(df))
    return (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=3, color=ACCENT)
        .encode(
            y=alt.Y(f"{category}:N", title=None, sort="-x"),
            x=alt.X(f"{value}:Q", title=None),
            tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip(f"{value}:Q", format=".2f")],
        )
        .properties(title=title, height=height)
    )


def _donut(df, category, value, title, *, color_mapping=None):
    return (
        alt.Chart(df)
        .mark_arc(innerRadius=62)
        .encode(
            theta=alt.Theta(f"{value}:Q"),
            color=_color(df, category, color_mapping or {}),
            tooltip=[alt.Tooltip(f"{category}:N"), alt.Tooltip(f"{value}:Q")],
        )
        .properties(title=title, height=300)
    )


def _hist(df, field, title, maxbins=20):
    return (
        alt.Chart(df)
        .mark_bar(color=ACCENT)
        .encode(
            x=alt.X(f"{field}:Q", bin=alt.Bin(maxbins=maxbins), title=field),
            y=alt.Y("count():Q", title="linhas"),
            tooltip=[alt.Tooltip("count():Q", title="linhas")],
        )
        .properties(title=title, height=300)
    )


def render_metric(name: str, df: pd.DataFrame) -> None:
    """Draw the curated chart for one metric table, tailored to its schema."""
    if name == "viagens_por_mes_status":
        _render(
            _bar(
                df, "mes", "total_viagens", "Viagens por mês × status",
                color="status", color_mapping=STATUS_COLORS,
            )
        )
    elif name == "taxa_atraso_mensal":
        _render(
            _line(df, "mes", "taxa_atraso", "Taxa de atraso mensal", pct=True, color=NEGATIVE)
        )
    elif name == "utilizacao_frota_mensal":
        _render(
            _line(
                df, "mes", "taxa_utilizacao", "Utilização da frota (mensal)",
                pct=True, color=POSITIVE,
            )
        )
    elif name == "top_motoristas":
        _render(_hbar(df, "motorista_nome", "viagens_concluidas", "Top motoristas"))
    elif name == "tempo_parado_geocercas":
        _render(_bar(df, "geocerca_tipo", "tempo_medio_parado_minutos", "Tempo parado (min)"))
    elif name == "tempo_medio_por_rota":
        plot = df.copy()
        origem = plot["origem_nome"].fillna(plot["geocerca_origem_id"])
        destino = plot["destino_nome"].fillna(plot["geocerca_destino_id"])
        plot["rota"] = origem + " → " + destino
        _render(_hbar(plot, "rota", "tempo_medio_horas", "Tempo médio por rota (h)"))


# ------------------------------------------------------------------ shared parts
def pending() -> None:
    st.warning(
        "Tabela ainda não materializada. Rode o pipeline "
        "(`docker compose up -d --build`) e recarregue."
    )


def render_quality(df: pd.DataFrame) -> None:
    if "dq_observations" not in df.columns:
        return
    flagged = df["dq_observations"].astype("string").fillna("")
    flagged = flagged[flagged.str.len() > 0]
    if flagged.empty:
        return
    reasons = flagged.str.split(";").explode().str.strip()
    counts = reasons.value_counts().rename_axis("motivo").reset_index(name="ocorrências")
    with st.expander(f"Observações de qualidade — {len(flagged)} linha(s) com flag"):
        st.dataframe(counts, use_container_width=True, hide_index=True)


def render_data(df: pd.DataFrame, name: str) -> None:
    with st.expander("Dados brutos"):
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button(
            "Baixar CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=f"{name}.csv",
            mime="text/csv",
            key=f"dl_{name}",
        )


def render_map(df: pd.DataFrame) -> None:
    points = df.dropna(subset=["latitude", "longitude"]).copy()
    if points.empty:
        st.caption("Sem coordenadas para exibir.")
        return
    points["cor"] = points["classificacao_localizacao"].map(GEO_COLORS).fillna("#888888")
    st.map(points, latitude="latitude", longitude="longitude", color="cor", size=30)


# ------------------------------------------------------------------------- pages
def page_overview(selected: list[str]) -> None:
    st.subheader("Visão geral")
    via = get("viagens_enriquecidas", selected)

    # A trip is "valid" when both its vehicle and driver resolved in the join.
    # Orphan trips (vehicle/driver dropped upstream in staging) stay in the fact
    # table but are excluded from every headline metric below.
    if via is not None and not via.empty:
        valid, invalid = split_valid(via)
    else:
        valid = invalid = None
    has_valid = valid is not None and not valid.empty

    cols = st.columns(5)
    kpi(cols[0], "Viagens válidas", fmt_int(len(valid)) if valid is not None else "—",
        accent=POSITIVE)
    kpi(cols[1], "Viagens inválidas", fmt_int(len(invalid)) if invalid is not None else "—",
        accent=NEGATIVE)
    kpi(cols[2], "Motoristas ativos",
        fmt_int(valid["motorista_id"].nunique()) if has_valid else "—")
    kpi(cols[3], "Taxa de atraso", fmt_pct(valid["atrasada_flag"].mean()) if has_valid else "—")
    kpi(cols[4], "Duração média",
        f"{valid['duracao_horas'].mean():.1f} h" if has_valid else "—")

    st.write("")
    names = list(METRICS)
    for i in range(0, len(names), 2):
        row = st.columns(2)
        for column, name in zip(row, names[i:i + 2]):
            with column:
                df = get(name, selected)
                if df is None or df.empty:
                    st.caption(f"**{name}** — ⏳ pendente")
                else:
                    render_metric(name, df)


def page_viagens(selected: list[str]) -> None:
    st.subheader("Viagens enriquecidas")
    st.caption(FACTS["viagens_enriquecidas"])
    df = get("viagens_enriquecidas", selected)
    if df is None:
        pending()
        return
    if df.empty:
        st.warning("Nenhuma linha para a seleção atual.")
        return

    # Metrics and charts use only valid trips (vehicle and driver resolved);
    # orphans stay visible in the quality breakdown and the raw table below.
    valid, invalid = split_valid(df)
    has_valid = not valid.empty

    cols = st.columns(5)
    kpi(cols[0], "Viagens válidas", fmt_int(len(valid)), accent=POSITIVE)
    kpi(cols[1], "Viagens inválidas", fmt_int(len(invalid)), accent=NEGATIVE)
    kpi(cols[2], "% atrasadas", fmt_pct(valid["atrasada_flag"].mean()) if has_valid else "—")
    kpi(cols[3], "Duração média",
        f"{valid['duracao_horas'].mean():.1f} h" if has_valid else "—")
    kpi(cols[4], "Velocidade média",
        f"{valid['velocidade_media_kmh'].mean():.1f} km/h" if has_valid else "—")

    if has_valid:
        left, right = st.columns(2)
        with left:
            status = valid.groupby("status").size().reset_index(name="viagens")
            _render(
                _donut(status, "status", "viagens", "Distribuição por status (válidas)",
                       color_mapping=STATUS_COLORS)
            )
        with right:
            _render(_hist(valid, "atraso_horas", "Distribuição do atraso (h, válidas)"))
        by_month = valid.groupby("mes").size().reset_index(name="viagens")
        _render(_bar(by_month, "mes", "viagens", "Viagens válidas por mês"))

    render_quality(df)
    render_data(df, "viagens_enriquecidas")


def page_posicoes(selected: list[str]) -> None:
    st.subheader("Posições × geocercas")
    st.caption(FACTS["posicoes_geocercas"])
    df = get("posicoes_geocercas", selected)
    if df is None:
        pending()
        return
    if df.empty:
        st.warning("Nenhuma linha para a seleção atual.")
        return

    cols = st.columns(4)
    kpi(cols[0], "Posições", fmt_int(len(df)))
    kpi(cols[1], "Em geocerca", fmt_pct((df["classificacao_localizacao"] == "em_geocerca").mean()))
    kpi(cols[2], "Velocidade média", f"{df['velocidade_kmh'].mean():.1f} km/h")
    events = int(df["evento_entrada_geocerca"].sum() + df["evento_saida_geocerca"].sum())
    kpi(cols[3], "Eventos entrada/saída", fmt_int(events))

    st.markdown("#### Mapa das posições")
    render_map(df)
    left, right = st.columns(2)
    with left:
        classes = df.groupby("classificacao_localizacao").size().reset_index(name="posicoes")
        _render(
            _donut(classes, "classificacao_localizacao", "posicoes", "Classificação",
                   color_mapping=GEO_COLORS)
        )
    with right:
        _render(_hist(df, "velocidade_kmh", "Distribuição de velocidade (km/h)"))

    render_quality(df)
    render_data(df, "posicoes_geocercas")


def page_metric(name: str, selected: list[str]) -> None:
    st.subheader(name)
    st.caption(METRICS[name])
    df = get(name, selected)
    if df is None:
        pending()
        return
    if df.empty:
        st.warning("Nenhuma linha para a seleção atual.")
        return
    render_metric(name, df)
    render_data(df, name)


# -------------------------------------------------------------------------- main
st.set_page_config(page_title="Logística · Gold", page_icon="🚚", layout="wide")
inject_css()

partitions = all_partitions()
st.title("🚚 Painel Analytics — camada gold")
st.caption(
    f"Fonte: `{ANALYTICS_ROOT}` · última ingest_date: "
    f"**{partitions[-1] if partitions else '—'}**"
)
if not partitions:
    st.warning(
        "Nenhuma tabela materializada ainda. Rode o pipeline "
        "(`docker compose up -d --build`) e recarregue."
    )

NAV = {"__overview__": "📊 Visão geral"}
NAV.update({name: f"🧾 {name}" for name in FACTS})
NAV.update({name: f"📈 {name}" for name in METRICS})

st.sidebar.header("Navegação")
selection = st.sidebar.radio(
    "Página", list(NAV), format_func=lambda key: NAV[key], label_visibility="collapsed"
)
st.sidebar.divider()
chosen_partitions = (
    st.sidebar.multiselect("Partições (ingest_date)", partitions, default=partitions)
    if partitions
    else []
)

if selection == "__overview__":
    page_overview(chosen_partitions)
elif selection == "viagens_enriquecidas":
    page_viagens(chosen_partitions)
elif selection == "posicoes_geocercas":
    page_posicoes(chosen_partitions)
else:
    page_metric(selection, chosen_partitions)
