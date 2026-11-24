import streamlit as st

def aplicar_estilo_visual():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    /* =============================================================
       PALETA -- extraída do layout feito no Claude Design.
       Fundo azul-petróleo bem escuro, acento ciano (não mais teal),
       alerta laranja, sucesso verde -- tudo em OKLCH, igual ao mockup.
       ============================================================= */
    :root, .stApp {
        --bg-page: oklch(15% 0.008 250);
        --bg-sidebar: oklch(13% 0.008 250);
        --bg-card: oklch(19% 0.01 250);
        --border: oklch(30% 0.01 250 / 0.55);
        --border-strong: oklch(30% 0.01 250 / 0.7);
        --text-primary: oklch(93% 0.004 250);
        --text-heading: oklch(96% 0.003 250);
        --text-muted: oklch(60% 0.01 250);
        --text-faint: oklch(45% 0.01 250);
        --accent: oklch(72% 0.1 210);
        --accent-strong: oklch(80% 0.09 210);
        --accent-tint: oklch(72% 0.1 210 / 0.14);
        --danger: oklch(68% 0.13 25);
        --danger-text: oklch(74% 0.11 25);
        --danger-tint: oklch(68% 0.13 25 / 0.08);
        --danger-border: oklch(68% 0.13 25 / 0.3);
        --success: oklch(72% 0.11 155);
        --success-tint: oklch(72% 0.11 155 / 0.14);
        --success-border: oklch(72% 0.11 155 / 0.4);

        --background-color: var(--bg-page) !important;
        --secondary-background-color: var(--bg-card) !important;
        --text-color: var(--text-primary) !important;
        --primary-color: var(--accent) !important;
    }
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"],
    [data-testid="stHeader"],
    .main {
        background-color: var(--bg-page) !important;
    }
    [data-testid="stHeader"] {
        background-color: rgba(0,0,0,0) !important;
    }
    .stApp {
        color: var(--text-primary);
    }

    html, body, [class*="css"] {
        font-family: 'Inter', system-ui, sans-serif;
    }
    h1, h2, h3, h4, h5, h6 {
        font-family: 'Inter', system-ui, sans-serif !important;
        font-weight: 600 !important;
        color: var(--text-heading) !important;
        letter-spacing: -0.015em;
    }
    /* Números tabulares (mesma largura por dígito) em vez de fonte mono --
       é a mesma técnica usada no mockup (.num{font-variant-numeric:tabular-nums}) */
    div[data-testid="stMetricValue"], .num-tabular {
        font-variant-numeric: tabular-nums;
    }

    div[data-testid="stMetric"], div[data-testid="metric-container"] {
        background: var(--bg-card) !important;
        border: 1px solid var(--border);
        border-radius: 14px;
        padding: 1.1rem 1.2rem;
        box-shadow: none;
    }
    div[data-testid="stMetricValue"] {
        font-family: 'Inter', system-ui, sans-serif !important;
        font-weight: 600 !important;
        color: var(--text-primary) !important;
        letter-spacing: -0.01em;
    }
    div[data-testid="stMetricLabel"] {
        font-weight: 500 !important;
        color: var(--text-muted) !important;
        font-size: 0.8rem !important;
        text-transform: none;
        letter-spacing: 0;
    }
    div[data-testid="stMetricLabel"] p {
        color: var(--text-muted) !important;
    }

    .stApp label, .stApp .stMarkdown, .stApp .stMarkdown p,
    .stApp [data-testid="stWidgetLabel"] p,
    .stApp [data-testid="stWidgetLabel"] {
        color: var(--text-primary) !important;
    }
    .stApp [data-testid="stCaptionContainer"] {
        color: var(--text-muted) !important;
    }

    section[data-testid="stSidebar"] {
        background: var(--bg-sidebar) !important;
        border-right: 1px solid var(--border);
    }
    section[data-testid="stSidebar"] * {
        color: var(--text-primary) !important;
    }

    /* Botões secundários (a maioria) -- estilo discreto com borda fina,
       igual ao mockup (nada de preenchimento sólido chamativo). */
    .stButton button,
    .stButton button[kind="secondary"],
    .stButton button:not([kind="primary"]) {
        background-color: var(--bg-card) !important;
        border: 1px solid var(--border-strong) !important;
        color: var(--text-primary) !important;
        border-radius: 9px !important;
        font-weight: 500 !important;
    }
    .stButton button *,
    .stButton button[kind="secondary"] *,
    .stButton button:not([kind="primary"]) * {
        color: var(--text-primary) !important;
    }
    .stButton button:hover,
    .stButton button:not([kind="primary"]):hover {
        background-color: oklch(22% 0.012 250) !important;
        border-color: var(--accent) !important;
        color: var(--text-primary) !important;
    }

    /* Botões primários (ação principal / item de menu ativo) -- usa o
       acento do mockup só que em preenchimento sólido, pra manter clareza
       de qual é a ação/página principal (o mockup usa esse acento como
       destaque translúcido; adaptei pra preenchimento sólido porque
       Streamlit precisa de contraste forte pra sinalizar 'isso é o principal'). */
    .stButton button[kind="primary"] {
        background-color: var(--accent) !important;
        border: 1px solid var(--accent) !important;
        color: oklch(15% 0.008 250) !important;
        border-radius: 9px !important;
        font-weight: 500 !important;
    }
    .stButton button[kind="primary"] * {
        color: oklch(15% 0.008 250) !important;
    }
    .stButton button[kind="primary"]:hover {
        background-color: var(--accent-strong) !important;
        border-color: var(--accent-strong) !important;
        color: oklch(15% 0.008 250) !important;
    }
    .stButton button[kind="primary"]:hover * {
        color: oklch(15% 0.008 250) !important;
    }

    section[data-testid="stSidebar"] .stButton button {
        width: 100%;
        text-align: left;
        justify-content: flex-start;
        border-radius: 8px !important;
        font-weight: 500;
        padding: 0.5rem 0.8rem;
    }
    /* Item de navegação ativo na sidebar: tinta translúcida do acento,
       igual ao navItem() do mockup -- não preenchimento sólido, porque
       aqui o botão é só rótulo de página, não uma ação a confirmar. */
    section[data-testid="stSidebar"] .stButton button[kind="primary"] {
        background-color: var(--accent-tint) !important;
        border: 1px solid transparent !important;
        color: var(--text-heading) !important;
    }
    section[data-testid="stSidebar"] .stButton button[kind="primary"] * {
        color: var(--text-heading) !important;
    }

    .nav-eyebrow {
        font-size: 0.68rem;
        font-weight: 600;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--text-faint) !important;
        margin: 1.1rem 0 0.4rem 0.3rem;
    }

    .stTabs [data-baseweb="tab-list"] {
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 500;
        color: var(--text-muted) !important;
    }
    .stTabs [data-baseweb="tab"] p {
        color: var(--text-muted) !important;
    }
    .stTabs [aria-selected="true"] {
        color: var(--accent-strong) !important;
    }
    .stTabs [aria-selected="true"] p {
        color: var(--accent-strong) !important;
    }

    div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
        border-radius: 12px;
        overflow: hidden;
        border: 1px solid var(--border);
    }

    div[data-testid="stExpander"], div[data-testid="stForm"] {
        border: 1px solid var(--border) !important;
        border-radius: 14px !important;
        background: var(--bg-card) !important;
    }

    /* -----------------------------------------------------------
       RESPONSIVO PARA MOBILE.
       st.columns() do Streamlit não empilha sozinho em tela estreita --
       fica tudo espremido lado a lado. Essa regra força empilhamento
       vertical abaixo de 640px (celular; não afeta tablet/desktop),
       e ajusta espaçamento/tamanho de fonte pra caber melhor.
       Não alcança a GRADE INTERNA do st.data_editor/st.dataframe --
       isso é limite real do componente, não tem CSS que resolva.
       ----------------------------------------------------------- */
    @media (max-width: 640px) {
        [data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
        }
        [data-testid="stHorizontalBlock"] > div {
            width: 100% !important;
            min-width: 100% !important;
            flex: 1 1 100% !important;
        }
        .block-container {
            padding-left: 0.9rem !important;
            padding-right: 0.9rem !important;
            padding-top: 1.2rem !important;
        }
        div[data-testid="stMetricValue"] { font-size: 1.25rem !important; }
        h1 { font-size: 1.3rem !important; }
        h2 { font-size: 1.15rem !important; }
        h3 { font-size: 1.05rem !important; }
        .stButton button {
            min-height: 2.6rem;
            font-size: 0.92rem !important;
        }
        section[data-testid="stSidebar"] .stButton button {
            min-height: 2.4rem;
        }
        div[data-testid="stDataFrame"], div[data-testid="stDataEditor"] {
            font-size: 0.85rem !important;
        }
    }
    </style>
    """, unsafe_allow_html=True)


def aplicar_tema_grafico(fig):
    # Plotly não entende oklch() nativamente -- estes hex são a conversão
    # matemática exata (OKLab -> sRGB) das mesmas cores usadas no CSS,
    # não uma aproximação visual.
    fig.update_layout(
        paper_bgcolor="#101418",
        plot_bgcolor="#101418",
        font=dict(family="Inter, sans-serif", color="#E6E8EA"),
        legend=dict(font=dict(color="#E6E8EA")),
        xaxis=dict(gridcolor="#2A2E33", linecolor="#2A2E33", color="#7C8186"),
        yaxis=dict(gridcolor="#2A2E33", linecolor="#2A2E33", color="#7C8186"),
    )
    return fig


def style_block_565():
    st.markdown("""
    <style>
    .ux-page-title { margin-bottom:.1rem; }
    .ux-subtitle { color:var(--text-muted); font-size:.9rem; margin-bottom:.8rem; }
    .ux-section-title { font-size:.76rem; font-weight:650; letter-spacing:.055em; text-transform:uppercase; color:var(--text-faint); margin:1.2rem 0 .5rem 0; }
    .ux-card { background:var(--bg-card); border:1px solid var(--border); border-radius:14px; padding:1rem 1.1rem; margin:.35rem 0; }
    .ux-card-strong { background:var(--bg-card); border:1px solid var(--accent); border-radius:14px; padding:1rem 1.1rem; margin:.35rem 0; }
    .ux-muted { color:var(--text-muted); font-size:.82rem; }
    .ux-value { font-variant-numeric:tabular-nums; font-size:1.35rem; font-weight:650; letter-spacing:-.015em; }
    .ux-row { background:var(--bg-card); border:1px solid var(--border); border-radius:11px; padding:.55rem .7rem; margin:.25rem 0; }
    .ux-badge { display:inline-block; border:1px solid var(--border-strong); border-radius:999px; padding:.12rem .48rem; font-size:.72rem; color:var(--text-muted); }
    .ux-badge-accent { display:inline-block; background:var(--accent-tint); border-radius:999px; padding:.12rem .48rem; font-size:.72rem; color:var(--accent-strong); }
    .ux-danger { color:var(--danger-text); }
    .ux-success { color:var(--success); }

    /* Tabelas de leitura do Demonstrativo: HTML próprio para evitar que o grid
       interno do st.dataframe force células brancas no tema escuro. */
    .ux-table-wrap { width:100%; overflow-x:auto; border:1px solid #2a3036; border-radius:10px; background:#101418; margin:.35rem 0 .75rem 0; }
    .ux-dark-table { width:100%; border-collapse:separate; border-spacing:0; min-width:720px; background:#101418; color:#e6e8ea; font-size:.82rem; }
    .ux-dark-table th { background:#23282f; color:#c2c8ce; font-weight:600; text-align:left; padding:.58rem .65rem; border-bottom:1px solid #343a40; border-right:1px solid #343a40; white-space:nowrap; }
    .ux-dark-table th:last-child, .ux-dark-table td:last-child { border-right:0; }
    .ux-dark-table td { background:#101418; color:#e6e8ea; padding:.55rem .65rem; border-bottom:1px solid #272d33; border-right:1px solid #272d33; vertical-align:middle; white-space:nowrap; }
    .ux-dark-table tbody tr:last-child td { border-bottom:0; }
    .ux-dark-table tbody tr:hover td { background:#182027; }
    .ux-dark-table tr.ux-row-paid td { background:#111c18; }
    .ux-dark-table tr.ux-row-pending td { background:#1b1811; }
    .ux-dark-table tr.ux-row-budget td { background:#121a20; }
    .ux-dark-table tr.ux-row-danger td { background:#211315; }
    .ux-dark-table tr.ux-row-warning td { background:#211d13; }
    .ux-dark-table td.ux-num, .ux-dark-table th.ux-num { text-align:right; font-variant-numeric:tabular-nums; }
    .ux-dark-table .ux-real-strong { font-weight:650; color:#f1f3f5; }
    .ux-dark-table .ux-plan-muted { color:#a6adb5; }
    [data-testid="stSidebar"] .stExpander { background:transparent !important; border:0 !important; }
    .ux-kpi { background:var(--bg-card); border:1px solid var(--border); border-radius:14px; padding:1rem 1.05rem; min-height:108px; }
    .ux-kpi-label { color:var(--text-muted); font-size:.78rem; font-weight:550; }
    .ux-kpi-value { margin-top:.3rem; font-size:1.42rem; font-weight:680; font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
    .ux-kpi-note { margin-top:.2rem; color:var(--text-muted); font-size:.75rem; }
    .ux-positive { color:var(--success) !important; }
    .ux-negative { color:var(--danger-text) !important; }
    .ux-accent { color:var(--accent-strong) !important; }
    .ux-flow-desc { font-weight:620; line-height:1.2; }
    .ux-flow-date { font-variant-numeric:tabular-nums; font-weight:620; margin-right:.35rem; }
    .ux-flow-category { color:var(--text-muted); font-size:.76rem; margin-top:.18rem; }
    .ux-flow-paid { opacity:.63; }
    .ux-flow-overdue { color:var(--danger-text); }
    .ux-flow-pending { color:var(--text-primary); }
    .ux-flow-value-main { font-variant-numeric:tabular-nums; font-weight:680; font-size:1rem; white-space:nowrap; }
    .ux-flow-value-sub { color:var(--text-muted); font-size:.72rem; white-space:nowrap; }
    .ux-group-icon { color:var(--text-muted); font-size:.8rem; margin-right:.3rem; }
    .ux-empty { text-align:center; border:1px dashed var(--border-strong); border-radius:14px; padding:1.35rem 1rem; color:var(--text-muted); background:var(--bg-card); }
    .ux-empty b { color:var(--text-primary); }
    .ux-empty-icon { font-size:1.35rem; color:var(--success); margin-bottom:.25rem; }
    .ux-payment-box { border-left:3px solid var(--accent); padding-left:.85rem; margin:.35rem 0 .65rem; }
    .ux-cover-summary { background:var(--bg-card); border:1px solid var(--border); border-radius:14px; padding:1rem 1.05rem; min-height:106px; }
    .ux-cover-label { color:var(--text-muted); font-size:.76rem; font-weight:600; }
    .ux-cover-value { margin-top:.28rem; font-size:1.32rem; font-weight:700; font-variant-numeric:tabular-nums; letter-spacing:-.02em; }
    .ux-cover-note { margin-top:.18rem; color:var(--text-muted); font-size:.74rem; }
    .ux-cover-alert { border:1px solid var(--danger-border); background:var(--danger-tint); border-radius:11px; padding:.65rem .8rem; margin:.65rem 0 1rem; color:var(--text-primary); }
    .ux-cover-ok { border:1px solid var(--success-border); background:var(--success-tint); border-radius:11px; padding:.65rem .8rem; margin:.65rem 0 1rem; color:var(--text-primary); }
    .ux-income-head { display:flex; justify-content:space-between; gap:1rem; align-items:flex-start; margin-bottom:.2rem; }
    .ux-income-name { font-weight:680; font-size:1rem; line-height:1.25; }
    .ux-income-meta { color:var(--text-muted); font-size:.76rem; margin-top:.14rem; }
    .ux-income-amount { font-size:1.08rem; font-weight:700; font-variant-numeric:tabular-nums; white-space:nowrap; }
    .ux-income-stats { display:flex; flex-wrap:wrap; gap:.45rem .9rem; margin:.6rem 0 .45rem; color:var(--text-muted); font-size:.76rem; }
    .ux-income-stats b { color:var(--text-primary); font-weight:620; }
    .ux-cover-bar { height:7px; border-radius:999px; overflow:hidden; background:#20262c; margin:.45rem 0 .65rem; }
    .ux-cover-fill-ok { height:100%; background:var(--success); }
    .ux-cover-fill-warn { height:100%; background:#d2a13a; }
    .ux-cover-fill-danger { height:100%; background:var(--danger); }
    .ux-match-line { display:grid; grid-template-columns:58px minmax(0,1fr) auto; gap:.55rem; align-items:center; padding:.48rem 0; border-top:1px solid var(--border); }
    .ux-match-date { color:var(--text-muted); font-size:.76rem; font-variant-numeric:tabular-nums; }
    .ux-match-desc { font-size:.84rem; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .ux-match-value { font-size:.82rem; font-weight:620; font-variant-numeric:tabular-nums; white-space:nowrap; }
    .ux-match-warning { color:var(--danger-text); font-size:.72rem; margin-top:.15rem; }
    .ux-source-badge { display:inline-block; border-radius:999px; padding:.1rem .45rem; font-size:.68rem; font-weight:650; margin-left:.35rem; vertical-align:1px; }
    .ux-source-received { background:var(--success-tint); color:var(--success); }
    .ux-source-planned { background:var(--accent-tint); color:var(--accent-strong); }
    .ux-source-risk { background:var(--danger-tint); color:var(--danger-text); }
    .ux-source-attn { background:rgba(210,161,58,.13); color:#d9ae55; }
    .ux-source-ok { background:var(--success-tint); color:var(--success); }
    .ux-secondary-note { color:var(--text-muted); font-size:.78rem; margin:.4rem 0 .8rem; }


    /* Fluxo 2.0 — agenda financeira com casamento renda → conta */
    .flow2-head { margin:.15rem 0 .7rem; }
    .flow2-title { font-size:1.7rem; font-weight:760; letter-spacing:-.035em; color:var(--text-heading); }
    .flow2-sub { margin-top:.2rem; color:var(--text-muted); font-size:.84rem; }
    .flow2-bridge { border:1px solid var(--success-border); background:linear-gradient(100deg, rgba(25,165,116,.10), var(--bg-card) 58%); border-radius:15px; padding:.9rem 1rem; margin:.55rem 0 1rem; }
    .flow2-bridge.warn { border-color:rgba(210,161,58,.38); background:linear-gradient(100deg, rgba(210,161,58,.10), var(--bg-card) 58%); }
    .flow2-bridge.danger { border-color:var(--danger-border); background:linear-gradient(100deg, var(--danger-tint), var(--bg-card) 58%); }
    .flow2-bridge-grid { display:grid; grid-template-columns:1.25fr .85fr auto; gap:1rem; align-items:center; }
    .flow2-bridge-label { color:var(--text-muted); font-size:.72rem; font-weight:650; }
    .flow2-bridge-name { margin-top:.17rem; font-size:1rem; font-weight:700; color:var(--text-heading); }
    .flow2-bridge-meta { margin-top:.12rem; color:var(--text-muted); font-size:.76rem; }
    .flow2-bridge-value { margin-top:.12rem; font-size:1.25rem; font-weight:720; font-variant-numeric:tabular-nums; }
    .flow2-pill { display:inline-block; border-radius:999px; padding:.28rem .64rem; font-size:.74rem; font-weight:700; white-space:nowrap; }
    .flow2-pill.ok { background:var(--success-tint); color:var(--success); }
    .flow2-pill.warn { background:rgba(210,161,58,.13); color:#d9ae55; }
    .flow2-pill.danger { background:var(--danger-tint); color:var(--danger-text); }
    .flow2-day { margin:1rem 0 .38rem; display:flex; align-items:center; gap:.45rem; color:var(--text-muted); font-size:.78rem; font-weight:700; letter-spacing:.015em; }
    .flow2-day.today { color:var(--accent-strong); }
    .flow2-dot { width:8px; height:8px; border-radius:50%; display:inline-block; background:var(--border-strong); }
    .flow2-dot.today { background:var(--accent); box-shadow:0 0 0 4px var(--accent-tint); }
    .flow2-row-anchor { display:none; }
    .flow2-name { color:var(--text-heading); font-size:.9rem; font-weight:680; line-height:1.15; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .flow2-meta { color:var(--text-muted); font-size:.72rem; margin-top:.18rem; line-height:1.25; }
    .flow2-meta.danger { color:var(--danger-text); }
    .flow2-meta.warn { color:#d9ae55; }
    .flow2-amount { font-size:1rem; font-weight:730; font-variant-numeric:tabular-nums; text-align:right; white-space:nowrap; }
    .flow2-match { margin-top:.18rem; text-align:right; color:var(--text-muted); font-size:.7rem; line-height:1.22; }
    .flow2-match.ok { color:var(--success); }
    .flow2-match.warn { color:#d9ae55; }
    .flow2-match.danger { color:var(--danger-text); }
    .flow2-paid { opacity:.58; }
    .flow2-income-details { margin:.35rem 0 .7rem 2.5rem; border-left:2px solid var(--accent); padding:.15rem 0 .15rem .8rem; }
    .flow2-income-line { display:grid; grid-template-columns:58px minmax(0,1fr) auto; gap:.55rem; align-items:center; padding:.38rem 0; border-bottom:1px solid var(--border); font-size:.76rem; }
    .flow2-income-line:last-child { border-bottom:0; }
    .flow2-selection { border:1px solid rgba(47,124,246,.34); background:linear-gradient(100deg, rgba(47,124,246,.12), var(--bg-card)); border-radius:13px; padding:.72rem .85rem; margin:.55rem 0 .9rem; }
    .flow2-selection strong { font-variant-numeric:tabular-nums; }
    .flow2-help { color:var(--text-muted); font-size:.73rem; margin:.35rem 0 .8rem; }
    .flow2-batch-note { border-left:3px solid var(--accent); padding-left:.75rem; margin:.35rem 0 .65rem; }

    /* Faz o container Streamlit das linhas se aproximar do card horizontal do mockup. */
    [data-testid="stVerticalBlockBorderWrapper"]:has(.flow2-row-anchor) {
        border-color:var(--border) !important; border-radius:13px !important; background:rgba(255,255,255,.012) !important;
    }
    [data-testid="stVerticalBlockBorderWrapper"]:has(.flow2-row-anchor):hover { border-color:var(--border-strong) !important; }

    /* Home 2.0 — orientação antes de análise */
    .home2-head { margin:.2rem 0 1rem; }
    .home2-hello { font-size:1.65rem; font-weight:760; letter-spacing:-.035em; color:var(--text-heading); line-height:1.08; }
    .home2-sub { margin-top:.32rem; color:var(--text-muted); font-size:.9rem; }
    .home2-period { display:inline-flex; align-items:center; gap:.35rem; border:1px solid var(--border-strong); border-radius:9px; padding:.4rem .65rem; color:var(--text-muted); font-size:.78rem; }
    .home2-hero { border:1px solid var(--success-border); background:linear-gradient(100deg, var(--success-tint), var(--bg-card) 55%); border-radius:16px; padding:1.15rem 1.25rem; margin:.35rem 0 1rem; }
    .home2-hero-grid { display:grid; grid-template-columns:1.05fr 1fr; gap:1.2rem; align-items:center; }
    .home2-hero-side { border-left:1px solid var(--border); padding-left:1.2rem; }
    .home2-hero.warn { border-color:rgba(210,161,58,.38); background:linear-gradient(100deg, rgba(210,161,58,.10), var(--bg-card) 55%); }
    .home2-hero.danger { border-color:var(--danger-border); background:linear-gradient(100deg, var(--danger-tint), var(--bg-card) 55%); }
    .home2-eyebrow { color:var(--text-muted); font-size:.74rem; font-weight:650; letter-spacing:.02em; }
    .home2-income-name { margin-top:.28rem; color:var(--text-heading); font-size:1.08rem; font-weight:700; }
    .home2-income-value { margin-top:.18rem; font-size:1.8rem; font-weight:760; font-variant-numeric:tabular-nums; color:var(--success); letter-spacing:-.035em; }
    .home2-income-date { color:var(--text-muted); font-size:.78rem; }
    .home2-bridge-value { margin-top:.2rem; font-size:1.4rem; font-weight:720; font-variant-numeric:tabular-nums; color:var(--text-heading); }
    .home2-status { margin-top:.55rem; border-radius:10px; padding:.55rem .7rem; font-size:.79rem; line-height:1.35; }
    .home2-status.ok { background:var(--success-tint); color:var(--text-primary); }
    .home2-status.warn { background:rgba(210,161,58,.13); color:var(--text-primary); }
    .home2-status.danger { background:var(--danger-tint); color:var(--text-primary); }
    .home2-panel { border:1px solid var(--border); background:var(--bg-card); border-radius:16px; padding:1rem 1.05rem; margin:.55rem 0; }
    .home2-panel-title { font-size:1rem; font-weight:700; color:var(--text-heading); margin-bottom:.55rem; }
    .home2-alert-row { border-radius:11px; padding:.68rem .75rem; margin:.36rem 0; border:1px solid var(--border); background:rgba(255,255,255,.012); }
    .home2-alert-row.danger { background:var(--danger-tint); border-color:var(--danger-border); }
    .home2-alert-row.warn { background:rgba(210,161,58,.08); border-color:rgba(210,161,58,.25); }
    .home2-alert-row.info { background:var(--accent-tint); border-color:rgba(80,180,220,.22); }
    .home2-alert-name { font-weight:670; color:var(--text-heading); font-size:.87rem; }
    .home2-alert-meta { color:var(--text-muted); font-size:.75rem; margin-top:.1rem; }
    .home2-month-card { border:1px solid var(--border); border-radius:13px; background:var(--bg-card); padding:.9rem .95rem; min-height:104px; }
    .home2-month-card.green { background:linear-gradient(140deg, var(--success-tint), var(--bg-card)); }
    .home2-month-card.red { background:linear-gradient(140deg, var(--danger-tint), var(--bg-card)); }
    .home2-month-card.blue { background:linear-gradient(140deg, var(--accent-tint), var(--bg-card)); }
    .home2-month-value { font-size:1.32rem; font-weight:730; font-variant-numeric:tabular-nums; letter-spacing:-.025em; }
    .home2-month-label { color:var(--text-muted); font-size:.76rem; margin-top:.16rem; }
    .home2-forecast { border-left:1px solid var(--border); padding-left:.9rem; min-height:104px; }
    .home2-forecast-title { color:var(--text-muted); font-size:.72rem; }
    .home2-forecast-line { font-size:.8rem; font-weight:620; margin-top:.38rem; font-variant-numeric:tabular-nums; }
    .home2-timeline { display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:.55rem; align-items:stretch; }
    .home2-event { border:1px solid var(--border); border-radius:11px; padding:.65rem .7rem; background:rgba(255,255,255,.012); min-height:94px; }
    .home2-event.today { border-color:rgba(80,180,220,.25); background:var(--accent-tint); }
    .home2-event.in { border-color:var(--success-border); background:var(--success-tint); }
    .home2-event.out { border-color:rgba(210,161,58,.26); background:rgba(210,161,58,.08); }
    .home2-event-date { font-size:.72rem; font-weight:700; color:var(--text-heading); }
    .home2-event-name { margin-top:.2rem; color:var(--text-muted); font-size:.72rem; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .home2-event-value { margin-top:.28rem; font-size:.8rem; font-weight:700; font-variant-numeric:tabular-nums; }
    .home2-quick-note { color:var(--text-muted); font-size:.75rem; margin-top:.1rem; }
    .home2-tip { border:1px solid rgba(80,180,220,.2); background:var(--accent-tint); border-radius:10px; padding:.55rem .75rem; color:var(--text-muted); font-size:.76rem; margin-top:.8rem; }
    @media (max-width:640px) {
      .ux-value { font-size:1.1rem; }
      .ux-card, .ux-card-strong { padding:.8rem .85rem; }
      .ux-kpi { min-height:88px; padding:.8rem .85rem; }
      .ux-kpi-value { font-size:1.16rem; }
      .ux-flow-category { display:none; }
      .ux-flow-value-main { font-size:.95rem; }
      .home2-hello { font-size:1.35rem; }
      .home2-income-value { font-size:1.5rem; }
      .home2-hero-grid { grid-template-columns:1fr; }
      .home2-hero-side { border-left:0; border-top:1px solid var(--border); padding-left:0; padding-top:.8rem; }
      .home2-timeline { grid-template-columns:1fr 1fr; }
      .home2-forecast { border-left:0; border-top:1px solid var(--border); padding-left:0; padding-top:.7rem; min-height:0; }
      .flow2-title { font-size:1.4rem; }
      .flow2-bridge-grid { grid-template-columns:1fr; gap:.55rem; }
      .flow2-bridge-value { font-size:1.08rem; }
      .flow2-income-details { margin-left:.35rem; }
      .flow2-income-line { grid-template-columns:48px minmax(0,1fr) auto; }
      [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) { flex-wrap:wrap !important; gap:.18rem !important; align-items:center !important; }
      [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(1) { flex:0 0 32px !important; min-width:32px !important; }
      [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(2) { flex:1 1 calc(100% - 42px) !important; min-width:180px !important; }
      [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(3) { flex:1 1 58% !important; min-width:150px !important; }
      [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(4) { flex:0 0 112px !important; min-width:112px !important; }
      [data-testid="stHorizontalBlock"]:has(.flow2-row-anchor) > div:nth-child(5) { flex:0 0 42px !important; min-width:42px !important; }

      .ux-dark-table { min-width:620px; font-size:.76rem; }
      .ux-dark-table th, .ux-dark-table td { padding:.48rem .52rem; }
      /* No Fluxo, mantém descrição e valor lado a lado; o botão desce inteiro. */
      [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) {
          flex-direction:row !important; flex-wrap:wrap !important; align-items:center !important; gap:.2rem !important;
      }
      [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div { min-width:0 !important; }
      [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(1) { flex:0 0 34px !important; width:34px !important; }
      [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(2) { flex:1 1 calc(100% - 154px) !important; width:auto !important; }
      [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(3) { flex:0 0 112px !important; width:112px !important; }
      [data-testid="stHorizontalBlock"]:has(.ux-flow-row-anchor) > div:nth-child(4) { flex:1 1 100% !important; width:100% !important; }
    }
    </style>
    """, unsafe_allow_html=True)


def style_block_781():
    st.markdown("""
    <style>
    .plan2-head { margin:.12rem 0 .8rem; }
    .plan2-title { font-size:1.7rem; font-weight:760; letter-spacing:-.035em; color:var(--text-heading); }
    .plan2-sub { margin-top:.22rem; color:var(--text-muted); font-size:.84rem; }
    .plan2-summary { border:1px solid var(--border); background:var(--bg-card); border-radius:16px; padding:1rem 1.05rem; min-height:154px; }
    .plan2-summary-top { display:flex; align-items:center; gap:.65rem; margin-bottom:.8rem; }
    .plan2-icon { width:38px; height:38px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:1.05rem; font-weight:800; }
    .plan2-icon.in { background:var(--success-tint); color:var(--success); }
    .plan2-icon.out { background:var(--danger-tint); color:var(--danger-text); }
    .plan2-icon.result { background:var(--accent-tint); color:var(--accent-strong); }
    .plan2-summary-name { font-weight:700; color:var(--text-heading); font-size:.94rem; }
    .plan2-pair { display:grid; grid-template-columns:1fr 1fr; gap:.7rem; }
    .plan2-small-label { color:var(--text-muted); font-size:.7rem; }
    .plan2-big { margin-top:.12rem; font-size:1.08rem; font-weight:720; font-variant-numeric:tabular-nums; }
    .plan2-delta { margin-top:.72rem; border-radius:9px; padding:.42rem .55rem; font-size:.76rem; font-weight:680; font-variant-numeric:tabular-nums; }
    .plan2-delta.good { background:var(--success-tint); color:var(--success); }
    .plan2-delta.bad { background:var(--danger-tint); color:var(--danger-text); }
    .plan2-delta.neutral { background:var(--accent-tint); color:var(--accent-strong); }
    .plan2-panel { border:1px solid var(--border); background:var(--bg-card); border-radius:16px; padding:1rem 1.05rem; margin:.65rem 0; }
    .plan2-panel-head { display:flex; justify-content:space-between; align-items:center; gap:1rem; margin-bottom:.55rem; }
    .plan2-panel-title { font-size:1rem; font-weight:720; color:var(--text-heading); }
    .plan2-panel-note { color:var(--text-muted); font-size:.72rem; }
    .plan2-row { display:grid; grid-template-columns:minmax(145px,.9fr) minmax(160px,1.55fr) minmax(145px,.7fr) minmax(120px,.58fr); gap:.8rem; align-items:center; padding:.62rem 0; border-top:1px solid var(--border); }
    .plan2-row:first-child { border-top:0; }
    .plan2-name { font-weight:650; color:var(--text-heading); font-size:.82rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .plan2-name-sub { margin-top:.1rem; color:var(--text-muted); font-size:.68rem; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .plan2-bar { height:7px; background:#20262c; border-radius:999px; overflow:hidden; }
    .plan2-fill { height:100%; border-radius:999px; }
    .plan2-fill.good { background:var(--success); }
    .plan2-fill.warn { background:#d2a13a; }
    .plan2-fill.bad { background:var(--danger); }
    .plan2-values { text-align:right; color:var(--text-muted); font-size:.73rem; font-variant-numeric:tabular-nums; white-space:nowrap; }
    .plan2-values b { color:var(--text-primary); font-weight:680; }
    .plan2-status { justify-self:end; border-radius:9px; padding:.28rem .48rem; font-size:.7rem; font-weight:680; white-space:nowrap; }
    .plan2-status.good { background:var(--success-tint); color:var(--success); }
    .plan2-status.warn { background:rgba(210,161,58,.13); color:#d9ae55; }
    .plan2-status.bad { background:var(--danger-tint); color:var(--danger-text); }
    .plan2-limit-row { display:grid; grid-template-columns:minmax(125px,.8fr) minmax(130px,1.1fr) 48px minmax(130px,.9fr); gap:.65rem; align-items:center; padding:.52rem 0; border-top:1px solid var(--border); }
    .plan2-limit-row:first-child { border-top:0; }
    .plan2-percent { font-size:.74rem; font-weight:720; font-variant-numeric:tabular-nums; text-align:right; }
    .plan2-debt-row { padding:.65rem 0; border-top:1px solid var(--border); }
    .plan2-debt-row:first-child { border-top:0; }
    .plan2-debt-head { display:flex; justify-content:space-between; align-items:flex-start; gap:.7rem; }
    .plan2-debt-name { font-weight:680; color:var(--text-heading); font-size:.84rem; }
    .plan2-debt-balance { font-weight:700; font-variant-numeric:tabular-nums; white-space:nowrap; }
    .plan2-debt-meta { display:flex; flex-wrap:wrap; gap:.35rem .8rem; color:var(--text-muted); font-size:.7rem; margin:.3rem 0 .35rem; }
    .plan2-debt-progress { height:6px; background:#20262c; border-radius:999px; overflow:hidden; }
    .plan2-debt-progress > span { display:block; height:100%; background:var(--accent); border-radius:999px; }
    @media (max-width:640px) {
      .plan2-title { font-size:1.4rem; }
      .plan2-summary { min-height:0; }
      .plan2-row { grid-template-columns:1fr auto; gap:.38rem .7rem; }
      .plan2-row .plan2-bar { grid-column:1 / -1; grid-row:2; }
      .plan2-row .plan2-values { grid-column:1; grid-row:3; text-align:left; }
      .plan2-row .plan2-status { grid-column:2; grid-row:3; }
      .plan2-limit-row { grid-template-columns:1fr auto; }
      .plan2-limit-row .plan2-bar { grid-column:1 / -1; }
      .plan2-limit-row .plan2-percent { text-align:left; }
    }
    </style>
    """, unsafe_allow_html=True)


def style_block_845():
    st.markdown("""
    <style>
    :root, .stApp {
      --bg-page:#071018;
      --bg-sidebar:#08121b;
      --bg-card:#0d1822;
      --bg-card-2:#101e29;
      --border:rgba(130,157,176,.16);
      --border-strong:rgba(130,157,176,.25);
      --text-primary:#eef5f7;
      --text-heading:#f7fbfc;
      --text-muted:#8da2b2;
      --text-faint:#607585;
      --accent:#2dd4bf;
      --accent-strong:#5eead4;
      --accent-tint:rgba(45,212,191,.14);
      --success:#39d98a;
      --success-tint:rgba(57,217,138,.12);
      --danger:#ff646f;
      --danger-text:#ff7b84;
      --danger-tint:rgba(255,100,111,.11);
    }

    /* Shell */
    .stApp, [data-testid="stAppViewContainer"], [data-testid="stMain"] {
      background:
        radial-gradient(900px 520px at 78% -12%, rgba(36,113,156,.12), transparent 62%),
        linear-gradient(180deg,#071018 0%,#081119 46%,#070e15 100%) !important;
    }
    .block-container {
      max-width:1500px !important;
      padding-top:1.65rem !important;
      padding-left:2rem !important;
      padding-right:2rem !important;
      padding-bottom:3rem !important;
    }
    section[data-testid="stSidebar"] {
      width:252px !important; min-width:252px !important;
      background:linear-gradient(180deg,#091520 0%,#08121b 66%,#071019 100%) !important;
      border-right:1px solid rgba(120,150,170,.13) !important;
      box-shadow:18px 0 45px rgba(0,0,0,.08);
    }
    section[data-testid="stSidebar"] > div { width:252px !important; }
    section[data-testid="stSidebar"] [data-testid="stSidebarContent"] { padding:1.25rem .78rem 1.1rem !important; }

    .brand2 { display:flex; align-items:center; gap:.72rem; padding:.22rem .3rem .55rem; }
    .brand2-mark {
      width:34px;height:34px;border-radius:11px;display:flex;align-items:center;justify-content:center;
      background:linear-gradient(145deg,rgba(45,212,191,.22),rgba(45,212,191,.06));
      border:1px solid rgba(45,212,191,.22); color:var(--accent-strong)!important;
      font-size:1.55rem;font-weight:800;line-height:1;transform:rotate(-12deg);
      box-shadow:0 8px 25px rgba(45,212,191,.08);
    }
    .brand2-name { font-size:1.02rem;font-weight:760;letter-spacing:-.02em;color:var(--text-heading)!important;line-height:1.12; }
    .brand2-sub { margin-top:.18rem;font-size:.68rem;color:var(--text-muted)!important; }
    .brand2-version { margin:.1rem .34rem .65rem; font-size:.65rem;color:var(--text-faint)!important; }
    .sidebar-period { text-align:center;padding:.47rem .15rem;font-weight:650;color:var(--text-heading)!important;font-size:.78rem; }

    .nav-eyebrow { margin:1.05rem .45rem .42rem !important;font-size:.62rem!important;letter-spacing:.11em!important;color:#617687!important; }
    section[data-testid="stSidebar"] hr { border-color:rgba(125,151,169,.13)!important;margin:1rem .25rem!important; }
    section[data-testid="stSidebar"] .stButton { margin:.14rem 0; }
    section[data-testid="stSidebar"] .stButton button {
      min-height:42px!important;padding:.55rem .72rem!important;border-radius:10px!important;
      background:transparent!important;border:1px solid transparent!important;color:#b7c6d1!important;
      font-size:.82rem!important;transition:background .14s ease,border-color .14s ease,transform .14s ease!important;
    }
    section[data-testid="stSidebar"] .stButton button:hover { background:rgba(255,255,255,.035)!important;border-color:rgba(126,154,174,.13)!important;transform:translateX(1px); }
    section[data-testid="stSidebar"] .stButton button[kind="primary"] {
      background:linear-gradient(90deg,rgba(45,212,191,.18),rgba(45,212,191,.105))!important;
      border:1px solid rgba(45,212,191,.18)!important;
      box-shadow:inset 3px 0 0 var(--accent),0 8px 24px rgba(0,0,0,.08)!important;
      color:#e9fffb!important;
    }
    section[data-testid="stSidebar"] .stButton button[kind="primary"] * { color:#e9fffb!important; }

    /* Controles */
    .stButton button { border-radius:10px!important; min-height:40px; }
    [data-baseweb="select"] > div, .stTextInput input, .stNumberInput input {
      background:#0c1720!important;border-color:rgba(127,155,175,.2)!important;border-radius:11px!important;
    }

    /* Cabeçalho do Planejamento */
    .plan2-shell-head { margin:.05rem 0 1.05rem; }
    .plan2-head { margin:.05rem 0 0!important; }
    .plan2-title { font-size:2rem!important;font-weight:760!important;letter-spacing:-.045em!important;line-height:1.02;color:#f8fbfc!important; }
    .plan2-sub { margin-top:.34rem!important;font-size:.86rem!important;color:#8ba0af!important; }
    [data-testid="stHorizontalBlock"]:has(.plan2-period-anchor) { align-items:flex-start!important; }
    [data-testid="stVerticalBlock"]:has(.plan2-period-anchor) [data-baseweb="select"] > div {
      background:linear-gradient(180deg,#10202c,#0e1b26)!important;border:1px solid rgba(116,151,174,.24)!important;
      min-height:44px!important;border-radius:13px!important;box-shadow:0 8px 28px rgba(0,0,0,.13);
    }
    [data-testid="stVerticalBlock"]:has(.plan2-period-anchor) [data-baseweb="select"] span { color:#eaf3f6!important;font-weight:600!important;font-size:.8rem!important; }

    /* Tabs em pills */
    .stTabs [data-baseweb="tab-list"] {
      width:max-content!important;max-width:100%;gap:3px!important;padding:4px!important;margin:.2rem 0 .9rem!important;
      background:#0c1923!important;border:1px solid rgba(119,149,169,.12)!important;border-radius:999px!important;
      box-shadow:inset 0 1px 0 rgba(255,255,255,.015);
    }
    .stTabs [data-baseweb="tab"] {
      height:38px!important;padding:0 1.05rem!important;border-radius:999px!important;border:1px solid transparent!important;
      background:transparent!important;color:#91a4b2!important;font-size:.79rem!important;
    }
    .stTabs [data-baseweb="tab"] p { color:inherit!important; }
    .stTabs [aria-selected="true"] {
      background:linear-gradient(180deg,rgba(45,212,191,.18),rgba(45,212,191,.10))!important;
      border-color:rgba(45,212,191,.55)!important;color:#effffc!important;
      box-shadow:0 0 0 1px rgba(45,212,191,.08),0 0 22px rgba(45,212,191,.09)!important;
    }
    .stTabs [data-baseweb="tab-highlight"], .stTabs [data-baseweb="tab-border"] { display:none!important; }

    /* Cards de resumo */
    .plan2-summary {
      position:relative;overflow:hidden;border:1px solid rgba(125,151,170,.16)!important;
      background:linear-gradient(155deg,rgba(18,34,46,.98),rgba(12,24,34,.98))!important;
      border-radius:17px!important;padding:1.05rem 1.1rem!important;min-height:171px!important;
      box-shadow:0 12px 30px rgba(0,0,0,.13),inset 0 1px 0 rgba(255,255,255,.018);
    }
    .plan2-summary::after { content:"";position:absolute;width:150px;height:150px;border-radius:50%;right:-65px;top:-78px;opacity:.24;filter:blur(1px); }
    .plan2-summary.income::after { background:radial-gradient(circle,rgba(45,212,191,.34),transparent 67%); }
    .plan2-summary.outcome::after { background:radial-gradient(circle,rgba(255,100,111,.29),transparent 67%); }
    .plan2-summary.result-card::after { background:radial-gradient(circle,rgba(56,189,248,.28),transparent 67%); }
    .plan2-summary-top { margin-bottom:.9rem!important;gap:.72rem!important;position:relative;z-index:1; }
    .plan2-icon { width:46px!important;height:46px!important;font-size:1.18rem!important;border:1px solid rgba(255,255,255,.035);box-shadow:inset 0 1px 0 rgba(255,255,255,.03); }
    .plan2-icon.in { background:rgba(45,212,191,.15)!important;color:#55e6d1!important; }
    .plan2-icon.out { background:rgba(255,100,111,.14)!important;color:#ff7d86!important; }
    .plan2-icon.result { background:rgba(56,189,248,.14)!important;color:#67cdf8!important; }
    .plan2-summary-name { font-size:.96rem!important;font-weight:720!important; }
    .plan2-pair { gap:0!important;position:relative;z-index:1; }
    .plan2-pair > div { padding-right:.85rem; }
    .plan2-pair > div + div { border-left:1px solid rgba(124,150,168,.15);padding-left:.95rem; }
    .plan2-small-label { font-size:.66rem!important;color:#8297a6!important; }
    .plan2-big { font-size:1.15rem!important;font-weight:750!important;color:#f2f7f9!important;margin-top:.16rem!important;letter-spacing:-.025em; }
    .plan2-delta {
      position:relative;z-index:1;margin-top:.85rem!important;border-radius:10px!important;padding:.48rem .65rem!important;
      font-size:.76rem!important;font-weight:720!important;
    }
    .plan2-delta.good { background:rgba(45,212,191,.12)!important;color:#53e4cf!important; }
    .plan2-delta.bad { background:rgba(255,100,111,.105)!important;color:#ff7e87!important; }
    .plan2-delta.neutral { background:rgba(89,132,161,.11)!important;color:#a9bfcd!important; }

    /* Painéis */
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.plan2-panel-anchor) {
      border:1px solid rgba(126,153,172,.15)!important;border-radius:17px!important;
      background:linear-gradient(155deg,rgba(14,28,39,.97),rgba(10,21,30,.98))!important;
      box-shadow:0 13px 34px rgba(0,0,0,.12),inset 0 1px 0 rgba(255,255,255,.016)!important;
      overflow:hidden!important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.plan2-panel-anchor) > div { padding:1rem 1.08rem!important; }
    .plan2-panel-anchor { display:block;width:0;height:0;overflow:hidden; }
    .plan2-panel-head { margin-bottom:.58rem!important;padding-bottom:.56rem;border-bottom:1px solid rgba(125,151,170,.12); }
    .plan2-panel-title { font-size:1.02rem!important;font-weight:730!important;letter-spacing:-.02em; }
    .plan2-panel-note { color:#768b9a!important;font-size:.68rem!important; }
    .plan2-empty-inline { padding:.75rem .05rem .25rem;color:#7f94a3;font-size:.76rem; }
    .plan2-row { grid-template-columns:minmax(150px,.92fr) minmax(210px,1.45fr) minmax(155px,.72fr) minmax(118px,.55fr)!important;gap:.9rem!important;padding:.68rem .12rem!important;border-color:rgba(126,153,172,.10)!important; }
    .plan2-name { font-size:.79rem!important;font-weight:680!important; }
    .plan2-name-cell { display:flex;align-items:center;gap:.62rem;min-width:0; }
    .plan2-cat-icon { width:30px;height:30px;flex:0 0 30px;border-radius:9px;display:flex;align-items:center;justify-content:center;background:#12232f;border:1px solid rgba(118,150,171,.13);color:#9fb6c5;font-size:.68rem;font-weight:760;box-shadow:inset 0 1px 0 rgba(255,255,255,.02); }
    .plan2-name-sub { font-size:.63rem!important;color:#718695!important; }
    .plan2-bar,.plan2-debt-progress { background:#1b2a35!important;box-shadow:inset 0 1px 2px rgba(0,0,0,.22); }
    .plan2-bar { height:7px!important; }
    .plan2-fill.good { background:linear-gradient(90deg,#34d8c1,#50dfca)!important; }
    .plan2-fill.warn { background:linear-gradient(90deg,#f3b74f,#ffc866)!important; }
    .plan2-fill.bad { background:linear-gradient(90deg,#ff6671,#ff7f87)!important; }
    .plan2-values { font-size:.69rem!important;color:#718695!important; }
    .plan2-values b { color:#eaf1f4!important;font-weight:700!important; }
    .plan2-status { border-radius:9px!important;padding:.34rem .55rem!important;font-size:.66rem!important;font-weight:700!important; }
    .plan2-status.good { background:rgba(45,212,191,.105)!important;color:#4ee0cb!important; }
    .plan2-status.warn { background:rgba(243,183,79,.12)!important;color:#f5bf5c!important; }
    .plan2-status.bad { background:rgba(255,100,111,.11)!important;color:#ff7e87!important; }

    .plan2-limit-row { grid-template-columns:minmax(120px,.8fr) minmax(120px,1.05fr) 46px minmax(120px,.9fr)!important;padding:.58rem .05rem!important;border-color:rgba(126,153,172,.10)!important; }
    .plan2-percent { font-size:.7rem!important;color:#dfe9ed; }
    .plan2-debt-row { padding:.72rem .05rem!important;border-color:rgba(126,153,172,.10)!important; }
    .plan2-debt-head { align-items:center!important; }
    .plan2-debt-name-wrap { display:flex;align-items:center;gap:.62rem;min-width:0; }
    .plan2-debt-icon { width:34px;height:34px;flex:0 0 34px;border-radius:10px;display:flex;align-items:center;justify-content:center;background:rgba(45,212,191,.09);border:1px solid rgba(45,212,191,.13);color:#52dfcb;font-size:.9rem; }
    .plan2-debt-name { font-size:.8rem!important;font-weight:700!important; }
    .plan2-debt-balance { font-size:.87rem!important;color:#f1f6f8!important; }
    .plan2-debt-meta { font-size:.64rem!important;color:#78909f!important;gap:.4rem .9rem!important;margin:.35rem 0 .45rem!important; }
    .plan2-debt-progress { height:6px!important; }
    .plan2-debt-progress > span { background:linear-gradient(90deg,#2dd4bf,#55e5d1)!important; }

    /* Filtros e expansores das abas secundárias */
    div[data-testid="stExpander"] { background:rgba(12,25,35,.75)!important;border-color:rgba(126,153,172,.15)!important; }

    @media (max-width:900px) {
      .block-container { padding-left:1.2rem!important;padding-right:1.2rem!important; }
      .plan2-title { font-size:1.7rem!important; }
      .plan2-row { grid-template-columns:1fr 1.4fr!important; }
      .plan2-row .plan2-values { text-align:left!important; }
      .plan2-row .plan2-status { justify-self:start!important; }
    }
    @media (max-width:640px) {
      section[data-testid="stSidebar"] { width:242px!important;min-width:242px!important; }
      section[data-testid="stSidebar"] > div { width:242px!important; }
      .block-container { padding-top:1rem!important;padding-left:.85rem!important;padding-right:.85rem!important; }
      .plan2-summary { min-height:0!important; }
      .plan2-row { grid-template-columns:1fr auto!important; }
      .stTabs [data-baseweb="tab"] { padding:0 .75rem!important; }
    }
    </style>
    """, unsafe_allow_html=True)


def style_block_1051():
    st.markdown("""
    <style>
    .income2-head { margin:.05rem 0 1.05rem; }
    .income2-title { font-size:2rem;font-weight:760;letter-spacing:-.045em;line-height:1.02;color:#f8fbfc; }
    .income2-sub { margin-top:.34rem;font-size:.86rem;color:#8ba0af; }
    [data-testid="stVerticalBlock"]:has(.income2-period-anchor) [data-baseweb="select"] > div {
      background:linear-gradient(180deg,#10202c,#0e1b26)!important;border:1px solid rgba(116,151,174,.24)!important;
      min-height:44px!important;border-radius:13px!important;box-shadow:0 8px 28px rgba(0,0,0,.13);
    }
    [data-testid="stVerticalBlock"]:has(.income2-period-anchor) [data-baseweb="select"] span { color:#eaf3f6!important;font-weight:600!important;font-size:.8rem!important; }
    .income2-kpi { position:relative;overflow:hidden;min-height:138px;padding:1rem 1.05rem;border-radius:17px;border:1px solid rgba(125,151,170,.16);background:linear-gradient(155deg,rgba(18,34,46,.98),rgba(12,24,34,.98));box-shadow:0 12px 30px rgba(0,0,0,.12),inset 0 1px 0 rgba(255,255,255,.018); }
    .income2-kpi-top { display:flex;align-items:center;gap:.72rem; }
    .income2-kpi-icon { width:43px;height:43px;border-radius:14px;display:flex;align-items:center;justify-content:center;font-size:1.12rem;font-weight:760; }
    .income2-kpi-icon.green { background:rgba(45,212,191,.15);color:#60ead6;border:1px solid rgba(45,212,191,.14); }
    .income2-kpi-icon.blue { background:rgba(56,189,248,.14);color:#65cef8;border:1px solid rgba(56,189,248,.14); }
    .income2-kpi-icon.amber { background:rgba(251,191,36,.14);color:#ffd064;border:1px solid rgba(251,191,36,.14); }
    .income2-kpi-icon.purple { background:rgba(167,139,250,.14);color:#b9a5ff;border:1px solid rgba(167,139,250,.14); }
    .income2-kpi-label { color:#c9d5dc;font-size:.78rem; }
    .income2-kpi-value { color:#f7fbfc;font-size:1.48rem;font-weight:760;letter-spacing:-.035em;margin-top:.08rem; }
    .income2-kpi-note { margin-top:.72rem;color:#869ba9;font-size:.7rem; }
    .income2-progress { height:8px;border-radius:99px;background:#142532;overflow:hidden;margin-top:.72rem; }
    .income2-progress span { display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,#2dd4bf,#5eead4); }
    .income2-progress.amber span { background:linear-gradient(90deg,#f5b84b,#ffd06d); }
    .income2-panel-anchor,.income2-source-anchor { display:block;width:0;height:0;overflow:hidden; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.income2-panel-anchor) { border:1px solid rgba(126,153,172,.15)!important;border-radius:17px!important;background:linear-gradient(155deg,rgba(14,28,39,.97),rgba(10,21,30,.98))!important;box-shadow:0 13px 34px rgba(0,0,0,.12),inset 0 1px 0 rgba(255,255,255,.016)!important;overflow:hidden!important; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.income2-panel-anchor) > div { padding:1rem 1.08rem!important; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.income2-source-anchor) { border:1px solid rgba(125,153,172,.13)!important;border-radius:14px!important;background:linear-gradient(155deg,rgba(15,30,41,.92),rgba(12,24,34,.96))!important;box-shadow:none!important;margin:.42rem 0!important; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.income2-source-anchor) > div { padding:.82rem .9rem!important; }
    .income2-panel-title { font-size:1.02rem;font-weight:730;letter-spacing:-.02em;color:#f2f7f9; }
    .income2-panel-note { font-size:.68rem;color:#768b9a; }
    .income2-source-name { font-size:.96rem;font-weight:720;color:#f1f6f8; }
    .income2-source-meta { margin-top:.18rem;font-size:.72rem;color:#8ca1af; }
    .income2-source-money { font-size:.8rem;color:#c8d6de; }
    .income2-source-money b { font-size:1.05rem;color:#f4f9fa; }
    .income2-badge { display:inline-flex;align-items:center;padding:.25rem .55rem;border-radius:999px;font-size:.64rem;font-weight:700;margin-left:.42rem; }
    .income2-badge.teal { background:rgba(45,212,191,.13);color:#59e4d0; }.income2-badge.blue { background:rgba(56,189,248,.12);color:#6ecdf4; }.income2-badge.amber { background:rgba(251,191,36,.12);color:#ffd069; }.income2-badge.purple { background:rgba(167,139,250,.12);color:#baa8ff; }
    .income2-status { display:inline-flex;padding:.28rem .58rem;border-radius:999px;font-size:.65rem;font-weight:720; }.income2-status.received { background:rgba(57,217,138,.12);color:#65e2a0; }.income2-status.expected { background:rgba(251,191,36,.12);color:#ffd16b; }.income2-status.partial { background:rgba(56,189,248,.12);color:#72d0f5; }
    .income2-timeline-row,.income2-progress-row { display:grid;align-items:center;gap:.7rem;padding:.66rem .05rem;border-top:1px solid rgba(124,151,169,.10); }.income2-timeline-row { grid-template-columns:62px minmax(130px,1fr) 105px 92px; }.income2-progress-row { grid-template-columns:minmax(130px,.9fr) minmax(150px,1.15fr) 150px; }
    .income2-date { color:#c9d6de;font-size:.74rem;font-weight:650; }.income2-name { color:#e9f1f4;font-size:.78rem;font-weight:620;white-space:nowrap;overflow:hidden;text-overflow:ellipsis; }.income2-value { color:#f4f8fa;font-size:.78rem;font-weight:720;text-align:right; }
    .income2-mini-bar { height:8px;border-radius:99px;background:#152632;overflow:hidden; }.income2-mini-bar span { display:block;height:100%;border-radius:99px;background:linear-gradient(90deg,#2dd4bf,#5eead4); }.income2-mini-values { color:#9db0bc;font-size:.7rem;text-align:right;white-space:nowrap; }
    .income2-special-copy b { color:#eff6f8;font-size:.8rem; }.income2-special-copy span { color:#8095a4;font-size:.68rem;display:block;margin-top:.12rem; }
    @media(max-width:800px){ .income2-title{font-size:1.55rem}.income2-timeline-row{grid-template-columns:54px 1fr auto}.income2-timeline-row .income2-status{display:none}.income2-progress-row{grid-template-columns:1fr auto}.income2-progress-row .income2-mini-bar{grid-column:1/-1;grid-row:2}.income2-mini-values{grid-column:2;grid-row:1} }
    </style>
    """, unsafe_allow_html=True)


def style_block_1225():
    st.markdown("""
    <style>
    .more2-head { margin:.05rem 0 1.05rem;display:flex;justify-content:space-between;gap:1rem;align-items:flex-start; }
    .more2-title { font-size:2rem;font-weight:760;letter-spacing:-.045em;line-height:1.02;color:#f8fbfc; }
    .more2-sub { margin-top:.34rem;font-size:.86rem;color:#8ba0af; }
    .more2-profile { min-width:190px;border:1px solid rgba(124,151,170,.18);background:linear-gradient(155deg,#10202c,#0d1a24);border-radius:15px;padding:.7rem .85rem;box-shadow:0 10px 28px rgba(0,0,0,.12); }
    .more2-profile-name { color:#f3f8fa;font-size:.82rem;font-weight:700; }
    .more2-profile-sub { margin-top:.12rem;color:#7f95a4;font-size:.68rem; }
    .more2-section { margin:.8rem 0 1.15rem; }
    .more2-section-head { display:flex;align-items:center;gap:.72rem;margin:0 0 .62rem;padding:.05rem .08rem; }
    .more2-section-icon { width:42px;height:42px;flex:0 0 42px;border-radius:13px;display:flex;align-items:center;justify-content:center;font-size:1.05rem;background:rgba(45,212,191,.10);border:1px solid rgba(45,212,191,.13); }
    .more2-section-title { font-size:1.05rem;font-weight:740;color:#f3f8fa;letter-spacing:-.02em; }
    .more2-section-sub { margin-top:.1rem;color:#7e93a2;font-size:.7rem; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-card-anchor) { border:1px solid rgba(126,153,172,.15)!important;border-radius:15px!important;background:linear-gradient(155deg,rgba(17,32,44,.97),rgba(11,23,32,.98))!important;box-shadow:0 10px 28px rgba(0,0,0,.11),inset 0 1px 0 rgba(255,255,255,.016)!important;overflow:hidden!important; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-card-anchor) > div { padding:.72rem .78rem!important; }
    .more2-card-anchor { display:block;width:0;height:0;overflow:hidden; }
    .more2-card-copy { display:flex;align-items:center;gap:.72rem;min-height:52px; }
    .more2-card-icon { width:42px;height:42px;flex:0 0 42px;border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.05rem;border:1px solid rgba(255,255,255,.035); }
    .more2-card-icon.blue { background:rgba(96,165,250,.13);color:#9ec5ff; }
    .more2-card-icon.green { background:rgba(52,211,153,.13);color:#72e6bc; }
    .more2-card-icon.purple { background:rgba(168,85,247,.13);color:#c9a3ff; }
    .more2-card-icon.pink { background:rgba(244,114,182,.13);color:#ffabd4; }
    .more2-card-icon.amber { background:rgba(251,191,36,.13);color:#ffd370; }
    .more2-card-icon.orange { background:rgba(251,146,60,.13);color:#ffb47c; }
    .more2-card-title { color:#f0f6f8;font-size:.8rem;font-weight:710;line-height:1.2; }
    .more2-card-desc { margin-top:.14rem;color:#788d9c;font-size:.66rem;line-height:1.3; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-card-anchor) .stButton button { min-height:42px!important;border-radius:11px!important;font-size:1.05rem!important;background:rgba(255,255,255,.018)!important;border-color:rgba(124,151,170,.12)!important;color:#91a7b6!important; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-card-anchor) .stButton button:hover { background:rgba(45,212,191,.08)!important;border-color:rgba(45,212,191,.28)!important;color:#6ee7d5!important; }
    .more2-section-anchor { display:block;width:0;height:0;overflow:hidden; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-section-anchor) { border:1px solid rgba(126,153,172,.13)!important;background:linear-gradient(155deg,rgba(12,25,35,.74),rgba(9,19,27,.78))!important;border-radius:18px!important;box-shadow:0 12px 32px rgba(0,0,0,.10),inset 0 1px 0 rgba(255,255,255,.012)!important;margin-bottom:1rem!important; }
    div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-section-anchor) > div { padding:1rem 1rem .72rem!important; }
    .more2-back { color:#84a0b1;font-size:.72rem;margin-bottom:.6rem; }
    .more2-mini-note { color:#728897;font-size:.7rem;line-height:1.45; }
    @media(max-width:700px){ .more2-head{display:block}.more2-profile{margin-top:.8rem;min-width:0}.more2-title{font-size:1.65rem}div[data-testid="stVerticalBlockBorderWrapper"]:has(.more2-section-anchor)>div{padding:.75rem .7rem .55rem!important} }
    </style>
    """, unsafe_allow_html=True)
