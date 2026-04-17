"""
Gestão Encontro com Deus — Streamlit + Supabase
Versão Final - White Theme + Fluxo Completo de Impressão e Reset
"""
import streamlit as st
import pandas as pd
import uuid, io, re, math, requests, unicodedata
from datetime import datetime, date, timezone
from supabase import create_client, Client
import streamlit.components.v1 as components

# ─── Config & Supabase ───────────────────────────────────────────────────────
def get_sb() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_KEY"])

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

# ─── Custom CSS (Light Mode / Clean SaaS) ────────────────────────────────────
def inject_custom_css():
    st.markdown("""
    <style>
    .stApp { background-color: #f8fafc; color: #1e293b; }
    div[data-testid="stContainer"] {
        border: 1px solid #e2e8f0 !important;
        border-radius: 10px !important;
        background-color: #ffffff !important;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
        padding: 1rem;
        margin-bottom: 10px;
    }
    .stButton>button[kind="primary"] {
        background-color: #2563eb !important;
        color: #ffffff !important;
        border-radius: 6px !important;
    }
    .stDownloadButton>button {
        background-color: #16a34a !important;
        color: #ffffff !important;
        border-radius: 6px !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ─── JS Helper para Abrir Múltiplos Links ────────────────────────────────────
def open_multiple_links(links):
    if not links: return
    js_code = f"""
    <script>
    const links = {links};
    links.forEach(link => {{
        window.open(link, '_blank');
    }});
    </script>
    """
    components.html(js_code, height=0)

# ─── Enums & Helpers ─────────────────────────────────────────────────────────
GENDER_MAP = {0: "-", 1: "Masculino", 2: "Feminino"}
GENDER_REV = {"Não informado": 0, "Masculino": 1, "Feminino": 2}
SHIRT_MAP = {0: "-", 1: "PP", 2: "P", 3: "M", 4: "G", 5: "GG", 6: "G1", 7: "G2", 8: "G3", 9: "G4"}
SHIRT_REV = {v: k for k, v in SHIRT_MAP.items()}
SHIRT_KEYS = ["P", "M", "G", "GG", "G1", "G2", "G3", "G4"]
CATEGORY_OPTIONS = ["Encontrista", "Servo", "Servo (sem camisa)", "Equipe"]

def norm(s):
    if not s: return ""
    return unicodedata.normalize("NFD", str(s)).encode("ascii", "ignore").decode().strip().lower()

def is_encounterist(cat): return "encontr" in norm(cat)
def is_server(cat): return "servo" in norm(cat)

def fmt_date_br(d):
    if not d: return ""
    try:
        parts = str(d).split("T")[0].split("-")
        return f"{parts[2]}/{parts[1]}/{parts[0]}"
    except: return str(d)

def parse_date_br(val):
    if pd.isna(val): return None
    s = str(val).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try: return datetime.strptime(s, fmt).date().isoformat()
        except: continue
    return None

def safe_str(val, max_len=None):
    if pd.isna(val): return None
    s = str(val).strip()
    return s[:max_len] if max_len else s

# ─── Data loaders ─────────────────────────────────────────────────────────────
def load_events(): return get_sb().table("Events").select("*").order("CreatedAtUtc", desc=True).execute().data or []
def load_event(eid): return get_sb().table("Events").select("*").eq("Id", eid).execute().data[0]
def load_participants(eid): return get_sb().table("Participants").select("*").eq("EventId", eid).order("Name").execute().data or []
def load_rooms(eid): return get_sb().table("Rooms").select("*").eq("EventId", eid).order("Name").execute().data or []
def load_assignments(eid): return get_sb().table("RoomAssignments").select("*").eq("EventId", eid).execute().data or []

def save_secretary_state(eid, team, dist, sec_status):
    import json
    payload = json.dumps({"team": team, "dist": dist, "status": sec_status})
    get_sb().table("Events").update({"SecretaryState": payload, "UpdatedAtUtc": utcnow()}).eq("Id", eid).execute()

def load_secretary_state(eid):
    import json
    ev = load_event(eid)
    if ev and ev.get("SecretaryState"):
        data = json.loads(ev["SecretaryState"])
        return data.get("team", []), data.get("dist", {}), data.get("status", {})
    return [], {}, {}

# ─── Google Sheets Integration ───────────────────────────────────────────────
def sheets_url_to_csv(url):
    if "export?format=csv" in url.lower(): return url
    m = re.search(r"spreadsheets/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m:
        sid = m.group(1); gid = "0"
        gm = re.search(r"[?#&]gid=([0-9]+)", url, re.I)
        if gm: gid = gm.group(1)
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&id={sid}&gid={gid}"
    return url

def fetch_sheet_csv(url):
    csv_url = sheets_url_to_csv(url)
    resp = requests.get(csv_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    if resp.status_code == 200: return resp.text
    raise Exception("Erro ao acessar planilha.")

def _do_load_letters(eid, url):
    try:
        text = fetch_sheet_csv(url)
        df = pd.read_csv(io.StringIO(text), dtype=str); df.columns = [c.strip() for c in df.columns]
        ct = "Para quem"; cm = "Mensagem"; cf = "De quem"
        letters = {}
        for _, row in df.iterrows():
            to = safe_str(row.get(ct)); msg = safe_str(row.get(cm))
            if to and msg: letters.setdefault(to, []).append({"sender": row.get(cf, "—"), "message": msg})
        st.session_state[f"letters_data_{eid}"] = letters
        st.session_state[f"letters_dl_{eid}"] = {"ts": datetime.now().strftime("%H:%M:%S"), "total": sum(len(v) for v in letters.values())}
    except: pass

def _do_load_photos(eid, url):
    try:
        text = fetch_sheet_csv(url)
        df = pd.read_csv(io.StringIO(text), dtype=str); df.columns = [c.strip() for c in df.columns]
        cn = "Nome do Encontrista"; cp = "Foto"
        groups = {}
        for _, row in df.iterrows():
            name = safe_str(row.get(cn)); photo = safe_str(row.get(cp))
            if name and photo:
                groups.setdefault(name, [])
                for lk in photo.split(","):
                    lk = lk.strip()
                    if lk: groups[name].append(lk)
        st.session_state[f"photo_groups_{eid}"] = groups
        st.session_state[f"photos_dl_{eid}"] = {"ts": datetime.now().strftime("%H:%M:%S"), "total": sum(len(v) for v in groups.values())}
    except: pass

def make_gdrive_view_url(raw_url):
    m = re.search(r"(?:id=|\/d\/)([A-Za-z0-9\-_]+)", raw_url)
    if m: return f"https://drive.google.com/file/d/{m.group(1)}/view"
    return raw_url

# ─── Geração de Documentos ───────────────────────────────────────────────────
def generate_letters_docx(participant_name, letters_list):
    from docx import Document; from docx.shared import Pt, Inches; from docx.enum.text import WD_ALIGN_PARAGRAPH
    doc = Document()
    for i, carta in enumerate(letters_list):
        if i > 0: doc.add_page_break()
        h = doc.add_paragraph(); h.alignment = WD_ALIGN_PARAGRAPH.CENTER
        h.add_run(f"Para: {participant_name}").bold = True
        doc.add_paragraph(f"De: {carta.get('sender','—')}").bold = True
        doc.add_paragraph(carta.get("message",""))
    buf = io.BytesIO(); doc.save(buf); buf.seek(0); return buf.getvalue()

# ─── PÁGINAS DO SISTEMA ───────────────────────────────────────────────────────

def page_dashboard(eid, ev):
    st.markdown(f"## 📊 Dashboard — {ev['Name']}")
    parts = load_participants(eid); enc = [p for p in parts if is_encounterist(p.get("Category"))]
    
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Encontristas", len(enc))
    c2.metric("Servos", sum(1 for p in parts if is_server(p.get("Category"))))
    c3.metric("Total Geral", len(parts)); c4.metric("Quartos", len(load_rooms(eid)))

    st.divider()
    col_t, col_b = st.columns([3,1])
    with col_t: st.markdown("### 📈 Recebimentos")
    with col_b:
        if st.button("🔄 Atualizar Dados", type="primary", use_container_width=True):
            if ev.get("LettersSheetUrl"): _do_load_letters(eid, ev["LettersSheetUrl"])
            if ev.get("PhotosSheetUrl"): _do_load_photos(eid, ev["PhotosSheetUrl"])
            st.rerun()

    letters = st.session_state.get(f"letters_data_{eid}", {})
    photo_groups = st.session_state.get(f"photo_groups_{eid}", {})

    def has_data(name, data_dict):
        return any(norm(name) == norm(k) or norm(name) in norm(k) or norm(k) in norm(name) for k in data_dict.keys())

    faltam_c = [p for p in enc if not has_data(p["Name"], letters)]
    faltam_f = [p for p in enc if not has_data(p["Name"], photo_groups)]

    try:
        import plotly.express as px
        c_chart1, c_chart2 = st.columns(2)
        with c_chart1:
            fig1 = px.pie(names=["Com Cartas", "Faltam"], values=[len(enc)-len(faltam_c), len(faltam_c)], hole=0.6, height=250, color_discrete_sequence=["#10b981", "#e2e8f0"])
            st.plotly_chart(fig1, use_container_width=True)
            st.markdown(f"**Faltam Cartas: {len(faltam_c)}**")
            st.dataframe(pd.DataFrame([{"Nome": p["Name"], "GC": p.get("ConnectionGroup", "-")} for p in faltam_c]), hide_index=True)
        with c_chart2:
            fig2 = px.pie(names=["Com Fotos", "Faltam"], values=[len(enc)-len(faltam_f), len(faltam_f)], hole=0.6, height=250, color_discrete_sequence=["#3b82f6", "#e2e8f0"])
            st.plotly_chart(fig2, use_container_width=True)
            st.markdown(f"**Faltam Fotos: {len(faltam_f)}**")
            st.dataframe(pd.DataFrame([{"Nome": p["Name"], "GC": p.get("ConnectionGroup", "-")} for p in faltam_f]), hide_index=True)
    except: st.warning("Instale 'plotly' para ver os gráficos.")

def page_letters(eid, ev):
    st.markdown("## 💌 Central de Cartas")
    parts = load_participants(eid); enc = [p for p in parts if is_encounterist(p.get("Category"))]
    letters = st.session_state.get(f"letters_data_{eid}", {})
    
    for p in enc:
        user_l = []
        for k, v in letters.items():
            if norm(p["Name"]) in norm(k) or norm(k) in norm(p["Name"]): user_l.extend(v)
        
        with st.container(border=True):
            col1, col2 = st.columns([4, 2])
            col1.markdown(f"**{p['Name']}** ({len(user_l)} cartas)")
            if user_l:
                doc = generate_letters_docx(p["Name"], user_l)
                col2.download_button("⬇️ Baixar Word", doc, f"Cartas_{p['Name']}.docx", use_container_width=True)
            else: col2.caption("Nenhuma carta")

def page_photos(eid, ev):
    st.markdown("## 📸 Central de Fotos")
    parts = load_participants(eid); enc = [p for p in parts if is_encounterist(p.get("Category"))]
    photo_groups = st.session_state.get(f"photo_groups_{eid}", {})
    
    for p in enc:
        user_p = []
        for k, v in photo_groups.items():
            if norm(p["Name"]) in norm(k) or norm(k) in norm(p["Name"]): user_p.extend(v)
        
        with st.container(border=True):
            col1, col2 = st.columns([4, 2])
            col1.markdown(f"**{p['Name']}** ({len(user_p)} fotos)")
            if user_p:
                if col2.button("🚀 Abrir Fotos", key=f"open_f_{p['Id']}", use_container_width=True):
                    open_multiple_links([make_gdrive_view_url(l) for l in user_p])
            else: col2.caption("Nenhuma foto")

def page_secretary(eid, ev):
    st.markdown("## 🗂️ Secretaria")
    parts = load_participants(eid); enc = [p for p in parts if is_encounterist(p.get("Category"))]
    team, dist, sec_status = load_secretary_state(eid)

    membro = st.selectbox("Selecione o Responsável", list(dist.keys()) if dist else ["(Vazio)"])
    pids = dist.get(membro, [])
    
    for p in [x for x in enc if x["Id"] in pids]:
        pid = p["Id"]; ps = sec_status.get(pid, {})
        with st.container(border=True):
            c1, c2, c3 = st.columns([3,2,2])
            c1.markdown(f"**{p['Name']}**")
            
            # Reset / Reiniciar
            if c3.button("🔄 Reiniciar", key=f"reset_{pid}"):
                sec_status[pid] = {"bolsa_ok": False, "print_status": "none"}
                save_secretary_state(eid, team, dist, sec_status); st.rerun()

            if ps.get("bolsa_ok"): c2.success("Finalizado")
            else:
                if c2.button("🖨️ Solicitar Impressão", key=f"req_{pid}"):
                    sec_status[pid]["print_status"] = "requested"
                    save_secretary_state(eid, team, dist, sec_status); st.rerun()

def page_print_management(eid, ev):
    st.markdown("## 🖨️ Gestão de Impressão (Letícia)")
    parts = load_participants(eid); _, _, sec_status = load_secretary_state(eid)
    letters = st.session_state.get(f"letters_data_{eid}", {})
    photos = st.session_state.get(f"photo_groups_{eid}", {})

    queue = [p for p in parts if sec_status.get(p["Id"], {}).get("print_status") in ["requested", "printing"]]
    
    if not queue: st.info("Fila vazia"); return

    for p in queue:
        with st.container(border=True):
            c1, c2, c3 = st.columns([3, 2, 2])
            c1.markdown(f"**{p['Name']}**")
            
            # Cartas Word
            user_l = []
            for k, v in letters.items():
                if norm(p["Name"]) in norm(k): user_l.extend(v)
            if user_l:
                doc = generate_letters_docx(p["Name"], user_l)
                c2.download_button("⬇️ Cartas (Word)", doc, f"Cartas_{p['Name']}.docx", on_click=lambda pid=p["Id"]: (sec_status.update({pid: {**sec_status.get(pid,{}), "print_status":"printing"}})))

            # Fotos Link
            user_p = []
            for k, v in photos.items():
                if norm(p["Name"]) in norm(k): user_p.extend(v)
            if user_p:
                if c3.button("🚀 Abrir Fotos (Abas)", key=f"prnt_f_{p['Id']}"):
                    open_multiple_links([make_gdrive_view_url(l) for l in user_p])

def main():
    st.set_page_config(page_title="ECD Gestão", page_icon="⛪", layout="wide")
    inject_custom_css()
    if "authenticated" not in st.session_state: st.session_state.authenticated = False
    if not st.session_state.authenticated:
        pwd = st.text_input("Senha", type="password")
        if st.button("Entrar"):
            if pwd == st.secrets.get("APP_PASSWORD", "encontro2025"): st.session_state.authenticated = True; st.rerun()
        return

    st.sidebar.title("⛪ ECD")
    page = st.sidebar.radio("Menu", ["📊 Dashboard", "👥 Participantes", "🏠 Quartos", "💌 Cartas", "📸 Fotos", "🗂️ Secretaria", "🖨️ Impressão", "⚙️ Config"])
    
    events = load_events()
    if not events: return
    eid = st.session_state.get("current_event", events[0]["Id"])
    ev = load_event(eid)

    pages = {
        "📊 Dashboard": page_dashboard,
        "💌 Cartas": page_letters,
        "📸 Fotos": page_photos,
        "🗂️ Secretaria": page_secretary,
        "🖨️ Impressão": page_print_management
    }
    pages.get(page, page_dashboard)(eid, ev)

if __name__ == "__main__": main()
