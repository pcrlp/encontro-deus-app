"""
Gestão Encontro com Deus — Streamlit + Supabase
Versão Final - Modo Claro (White Theme) + Dashboard Atualizado
"""
import streamlit as st
import pandas as pd
import uuid, io, re, math, requests, unicodedata
from datetime import datetime, date, timezone
from supabase import create_client, Client

# ─── Config & Supabase ───────────────────────────────────────────────────────
def get_sb() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_KEY"])

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

# ─── Custom CSS (Light Mode / Clean SaaS) ────────────────────────────────────
def inject_custom_css():
    st.markdown("""
    <style>
    /* Força textos para cor escura e fundo principal claro caso o Streamlit tente misturar */
    .stApp { background-color: #f8fafc; color: #1e293b; }
    
    /* Neumorphic/SaaS Containers - Light */
    div[data-testid="stContainer"] {
        border: 1px solid #e2e8f0 !important;
        border-radius: 10px !important;
        background-color: #ffffff !important;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1), 0 1px 2px 0 rgba(0, 0, 0, 0.06);
        transition: all 0.2s ease;
        padding: 1rem;
    }
    div[data-testid="stContainer"]:hover {
        border-color: #cbd5e1 !important;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    }

    /* Primary Buttons - Blue */
    .stButton>button[kind="primary"] {
        background-color: #2563eb !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        border: none !important;
        border-radius: 6px !important;
    }
    .stButton>button[kind="primary"]:hover {
        background-color: #1d4ed8 !important;
    }
    
    /* Download Buttons - Green */
    .stDownloadButton>button {
        background-color: #16a34a !important;
        color: #ffffff !important;
        font-weight: 600 !important;
        border: none !important;
        border-radius: 6px !important;
    }
    .stDownloadButton>button:hover {
        background-color: #15803d !important;
    }
    
    /* Ajuste de cor nos expanders para modo claro */
    .streamlit-expanderHeader {
        background-color: #f1f5f9 !important;
        border-radius: 6px;
        color: #0f172a !important;
    }
    </style>
    """, unsafe_allow_html=True)

# ─── Enums ────────────────────────────────────────────────
GENDER_MAP = {0: "-", 1: "Masculino", 2: "Feminino"}
GENDER_REV = {"Não informado": 0, "Masculino": 1, "Feminino": 2}

SHIRT_MAP = {0: "-", 1: "PP", 2: "P", 3: "M", 4: "G", 5: "GG", 6: "G1", 7: "G2", 8: "G3", 9: "G4"}
SHIRT_REV = {v: k for k, v in SHIRT_MAP.items()}
SHIRT_KEYS = ["P", "M", "G", "GG", "G1", "G2", "G3", "G4"]

CATEGORY_OPTIONS = ["Encontrista", "Servo", "Servo (sem aquisição de camisa)", "Equipe"]

KIND_MAP = {0: "Palestra", 1: "Louvor", 2: "Intervalo", 3: "Refeição", 4: "Dinâmica", 5: "Outro"}
KIND_REV = {v: k for k, v in KIND_MAP.items()}

# ─── Helpers ──────────────────────────────────────────────────────────────────
def fmt_date_br(d):
    if not d: return ""
    try:
        parts = str(d).split("T")[0].split("-")
        return f"{parts[2]}/{parts[1]}/{parts[0]}"
    except: return str(d)

def norm(s):
    if not s: return ""
    return unicodedata.normalize("NFD", str(s)).encode("ascii", "ignore").decode().strip().lower()

def is_encounterist(cat): return "encontr" in norm(cat)
def is_server(cat): return "servo" in norm(cat)
def is_server_no_shirt(cat):
    c = norm(cat); return is_server(cat) and ("sem aquisicao" in c or "sem camisa" in c)
def is_server_with_shirt(cat): return is_server(cat) and not is_server_no_shirt(cat)

def age_from(birth_str):
    if not birth_str: return None
    try:
        d = str(birth_str).split("T")[0]; parts = d.split("-")
        y, m, day = int(parts[0]), int(parts[1]), int(parts[2])
        today = date.today(); age = today.year - y
        if (today.month, today.day) < (m, day): age -= 1
        return abs(age)
    except: return None

def parse_gender(val):
    if pd.isna(val): return 0
    s = str(val).strip().lower()
    if s in ("m", "masculino", "male", "1"): return 1
    if s in ("f", "feminino", "female", "2"): return 2
    return 0

def parse_shirt(val):
    if pd.isna(val): return 0
    return SHIRT_REV.get(str(val).strip().upper(), 0)

def safe_str(val, max_len=None):
    if pd.isna(val): return None
    s = str(val).strip()
    if not s or s == "nan": return None
    if max_len: s = s[:max_len]
    return s

def parse_date_br(val):
    if pd.isna(val): return None
    s = str(val).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%y"):
        try: return datetime.strptime(s, fmt).date().isoformat()
        except: continue
    return None

# ─── Data loaders ─────────────────────────────────────────────────────────────
def load_events(): return get_sb().table("Events").select("*").order("CreatedAtUtc", desc=True).execute().data or []
def load_event(eid):
    r = get_sb().table("Events").select("*").eq("Id", eid).execute(); return r.data[0] if r.data else None
def load_participants(eid): return get_sb().table("Participants").select("*").eq("EventId", eid).order("Name").execute().data or []
def load_rooms(eid): return get_sb().table("Rooms").select("*").eq("EventId", eid).order("Name").execute().data or []
def load_assignments(eid): return get_sb().table("RoomAssignments").select("*").eq("EventId", eid).execute().data or []

def save_secretary_state(eid, team, dist, sec_status):
    import json
    payload = json.dumps({"team": team, "dist": dist, "status": sec_status})
    try: get_sb().table("Events").update({"SecretaryState": payload, "UpdatedAtUtc": utcnow()}).eq("Id", eid).execute()
    except: pass

def load_secretary_state(eid):
    import json
    try:
        ev = load_event(eid)
        if ev and ev.get("SecretaryState"):
            data = json.loads(ev["SecretaryState"])
            return data.get("team", []), data.get("dist", {}), data.get("status", {})
    except: pass
    return [], {}, {}

# ─── CSV Import ───────────────────────────────────────────────────────────────
def find_header(columns, candidates):
    for c in candidates:
        for col in columns:
            if norm(col) == norm(c): return col
    return None

def import_csv(event_id, file_bytes, replace=False):
    sb = get_sb()
    if replace:
        sb.table("RoomAssignments").delete().eq("EventId", event_id).execute()
        sb.table("Participants").delete().eq("EventId", event_id).execute()
    try: text = file_bytes.decode("utf-8-sig")
    except:
        try: text = file_bytes.decode("latin-1")
        except: text = file_bytes.decode("cp1252", errors="replace")
    first_line = text.split("\n")[0]
    delim = ";" if first_line.count(";") > first_line.count(",") else ","
    df = pd.read_csv(io.StringIO(text), sep=delim, dtype=str); df.columns = [c.strip() for c in df.columns]

    col_name    = find_header(df.columns, ["Nome do Encontrista", "Encontrista", "Nome", "Participante"])
    col_cat     = find_header(df.columns, ["Categoria"])
    col_email   = find_header(df.columns, ["Email", "E-mail"])
    col_gender  = find_header(df.columns, ["Sexo"])
    col_shirt   = find_header(df.columns, ["Tamanho da Camisa", "Tamanho da Camisa ", "Camiseta", "Camisa"])
    col_birth   = find_header(df.columns, ["Data de Nascimento", "Nascimento"])
    col_phone   = find_header(df.columns, ["Celular", "Telefone", "Celular (WhatsApp)", "Celular/WhatsApp", "Telefone Celular", "Celular / WhatsApp", "Contato (celular)"])
    col_marital = find_header(df.columns, ["Estado civil", "Estado Civil"])
    col_sector  = find_header(df.columns, ["Qual nome do setor do grupo de conexão?", "Qual o nome do setor do grupo de conexão?", "Setor de Conexão", "Setor de Conexao", "Setor (Centro, Norte...)", "Setor"])
    col_group   = find_header(df.columns, ["Nome do Grupo de Conexão", "Se a sua resposta anterior foi afirmativa, qual é o nome do seu GC?", "Qual é o nome do seu GC", "nome do seu gc", "Grupo de Conexão", "Grupo de Conexao", "Nome do GC", "GC"])
    col_invited = find_header(df.columns, ["Você veio a convite de alguém? Se sim, poderia me informar o nome da pessoa que o(a) convidou?", "Voce veio a convite de alguem", "Quem convidou", "Convidado por", "Indicado por", "Indicador por", "Quem indicou"])

    if not col_name: raise ValueError(f"Coluna 'Nome' não encontrada. Colunas: {', '.join(df.columns)}")
    ok, fail = 0, 0
    for _, row in df.iterrows():
        name = safe_str(row.get(col_name) if col_name else None, 200)
        if not name: fail += 1; continue
        record = {"Id": str(uuid.uuid4()), "EventId": event_id, "Name": name,
            "Email": safe_str(row.get(col_email) if col_email else None, 200),
            "Gender": parse_gender(row.get(col_gender) if col_gender else None),
            "BirthDate": parse_date_br(row.get(col_birth) if col_birth else None),
            "Phone": safe_str(row.get(col_phone) if col_phone else None, 40),
            "ShirtSize": parse_shirt(row.get(col_shirt) if col_shirt else None),
            "Category": safe_str(row.get(col_cat) if col_cat else None, 120),
            "ConnectionSector": safe_str(row.get(col_sector) if col_sector else None, 120),
            "ConnectionGroup": safe_str(row.get(col_group) if col_group else None, 120),
            "InvitedBy": safe_str(row.get(col_invited) if col_invited else None, 200),
            "MaritalStatus": safe_str(row.get(col_marital) if col_marital else None, 60),
            "CreatedAtUtc": utcnow()}
        record = {k: v for k, v in record.items() if v is not None}
        try: sb.table("Participants").insert(record).execute(); ok += 1
        except: fail += 1
    return ok, fail

# ─── Auto-distribute rooms ───────────────────────────────────────────────────
def distribute_rooms(event_id):
    sb = get_sb()
    sb.table("RoomAssignments").delete().eq("EventId", event_id).execute()
    all_rooms = load_rooms(event_id); parts = load_participants(event_id)
    if not all_rooms: return 0, 0, "Nenhum quarto cadastrado."
    enc = [p for p in parts if is_encounterist(p.get("Category"))]
    if not enc: return 0, 0, "Nenhum encontrista encontrado."

    def distrib(gv):
        rooms   = sorted([r for r in all_rooms if r.get("Gender") == gv], key=lambda r: r["Capacity"])
        people  = [p for p in enc if p.get("Gender") == gv]
        if not rooms or not people: return []
        seniors = sorted([p for p in people if (age_from(p.get("BirthDate")) or 0) >= 50], key=lambda p: -(age_from(p.get("BirthDate")) or 0))
        others  = sorted([p for p in people if (age_from(p.get("BirthDate")) or 0) <  50], key=lambda p: -(age_from(p.get("BirthDate")) or 0))
        total_cap = sum(r["Capacity"] for r in rooms); total_p = len(people)
        if total_cap <= 0: return []
        target = {r["Id"]: int(math.floor(total_p * r["Capacity"] / total_cap)) for r in rooms}
        rem = total_p - sum(target.values())
        order = sorted(rooms, key=lambda r: -(r["Capacity"] - target[r["Id"]])); idx = 0
        while rem > 0: target[order[idx % len(order)]["Id"]] += 1; rem -= 1; idx += 1
        assigns = []
        for room in rooms:
            need = min(room["Capacity"], target[room["Id"]])
            while need > 0 and seniors:
                assigns.append({"Id": str(uuid.uuid4()), "EventId": event_id, "RoomId": room["Id"], "ParticipantId": seniors.pop(0)["Id"], "CreatedAtUtc": utcnow()}); need -= 1
            while need > 0 and others:
                assigns.append({"Id": str(uuid.uuid4()), "EventId": event_id, "RoomId": room["Id"], "ParticipantId": others.pop(0)["Id"], "CreatedAtUtc": utcnow()}); need -= 1
        return assigns

    inserts = distrib(2) + distrib(1)
    for a in inserts: sb.table("RoomAssignments").insert(a).execute()
    return len(inserts), len(all_rooms), None

# ─── PDF Generators ───────────────────────────────────────────────────────────
def generate_sector_pdf(participants, ev_name="Encontro com Deus", assigns=None, rooms=None):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    assigns = assigns or []; rooms = rooms or []
    rm = {r["Id"]: r["Name"] for r in rooms}
    pr = {a["ParticipantId"]: rm.get(a["RoomId"], "-") for a in assigns}

    SC = {"azul": "#DBEAFE", "amarelo": "#FEF9C3", "verde": "#DCFCE7", "lilas": "#F3E8FF", "vermelho": "#FEE2E2"}
    def sk(s):
        n = norm(s)
        for k in SC:
            if n.startswith(k): return k
        return ""
    ORDER = ["azul", "amarelo", "verde", "lilas", "vermelho", ""]

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=30, rightMargin=30, topMargin=36, bottomMargin=36)
    styles    = getSampleStyleSheet()
    cell_style = ParagraphStyle("cell", fontName="Helvetica",      fontSize=8, leading=10)
    hdr_style  = ParagraphStyle("hdr",  fontName="Helvetica-Bold", fontSize=8, leading=10)
    title_style = ParagraphStyle("title2", fontName="Helvetica-Bold", fontSize=13, leading=16, spaceAfter=4)
    sub_style   = ParagraphStyle("sub",   fontName="Helvetica", fontSize=9, leading=11, textColor=colors.HexColor("#555555"))

    def build_section(group_label, group_parts):
        elems = []
        elems.append(Paragraph(ev_name, title_style))
        elems.append(Paragraph(f"{group_label} — {len(group_parts)} pessoa(s)", sub_style))
        elems.append(Paragraph(f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}", sub_style))
        elems.append(Spacer(1, 10))

        sorted_parts = sorted(group_parts, key=lambda p: (
            ORDER.index(sk(p.get("ConnectionSector", ""))), p.get("Name", "")
        ))

        data = [[
            Paragraph("Nº",           hdr_style),
            Paragraph("Nome",         hdr_style),
            Paragraph("Categoria",    hdr_style),
            Paragraph("Quarto",       hdr_style),
            Paragraph("Setor / GC",   hdr_style),
            Paragraph("Convidado por",hdr_style),
        ]]
        row_colors = []

        for i, p in enumerate(sorted_parts):
            sector   = p.get("ConnectionSector") or ""
            gc       = p.get("ConnectionGroup") or "-"
            setor_gc = f"{sector or '-'} / {gc}" if sector else gc
            quarto   = pr.get(p["Id"], "-")
            invited  = p.get("InvitedBy") or "-"
            cat      = p.get("Category") or "-"
            data.append([
                Paragraph(str(i+1), cell_style),
                Paragraph(p.get("Name", ""), cell_style),
                Paragraph(cat,      cell_style),
                Paragraph(quarto,   cell_style),
                Paragraph(setor_gc, cell_style),
                Paragraph(invited,  cell_style),
            ])
            row_colors.append(colors.HexColor(SC.get(sk(sector), "#F9FAFB")))

        col_widths = [22, 145, 75, 65, 118, 110]
        t = Table(data, colWidths=col_widths, repeatRows=1)
        table_style = [
            ("BACKGROUND",    (0,0), (-1,0), colors.HexColor("#E2E8F0")),
            ("GRID",          (0,0), (-1,-1), 0.4, colors.HexColor("#CBD5E1")),
            ("VALIGN",        (0,0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING",   (0,0), (-1,-1), 3),
            ("RIGHTPADDING",  (0,0), (-1,-1), 3),
        ]
        for i, c in enumerate(row_colors):
            table_style.append(("BACKGROUND", (0, i+1), (-1, i+1), c))
        t.setStyle(TableStyle(table_style))
        elems.append(t)
        return elems

    encontristas = [p for p in participants if is_encounterist(p.get("Category"))]
    servos       = [p for p in participants if is_server(p.get("Category"))]
    equipe       = [p for p in participants if norm(p.get("Category","")).startswith("equipe")]
    outros       = [p for p in participants if not is_encounterist(p.get("Category")) and not is_server(p.get("Category")) and not norm(p.get("Category","")).startswith("equipe")]

    all_elems = []; sections = []
    if encontristas: sections.append(("Encontristas", encontristas))
    if servos:       sections.append(("Servos", servos))
    if equipe:       sections.append(("Equipe", equipe))
    if outros:       sections.append(("Outros", outros))
    if not sections: sections = [("Participantes", participants)]

    for idx, (label, grp) in enumerate(sections):
        if idx > 0: all_elems.append(PageBreak())
        all_elems.extend(build_section(label, grp))

    doc.build(all_elems)
    return buf.getvalue()

def generate_rooms_pdf(event_id):
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    rooms   = load_rooms(event_id)
    assigns = load_assignments(event_id)
    parts   = load_participants(event_id)
    pm      = {p["Id"]: p for p in parts}
    abr     = {}
    for a in assigns:
        abr.setdefault(a["RoomId"], []).append(a)

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=36, rightMargin=36,
                            topMargin=36,  bottomMargin=36)
    styles = getSampleStyleSheet()

    cell_style = ParagraphStyle("rcell", fontName="Helvetica",      fontSize=8, leading=10)
    hdr_style  = ParagraphStyle("rhdr",  fontName="Helvetica-Bold", fontSize=8, leading=10)

    COL_W = [210, 32, 40, 145, 96]

    elems = []
    for ri, room in enumerate(rooms):
        if ri > 0:
            elems.append(PageBreak())
        leader = pm.get(room.get("LeaderId")) if room.get("LeaderId") else None

        elems.append(Paragraph(f"Quarto: {room['Name']}", styles["Title"]))
        elems.append(Paragraph(
            f"Sexo: {GENDER_MAP.get(room.get('Gender', 0), '-')} · "
            f"Capacidade: {room['Capacity']} · "
            f"Líder: {leader['Name'] if leader else '-'}",
            styles["Normal"]))
        elems.append(Spacer(1, 12))

        data = [[
            Paragraph("Nome",   hdr_style),
            Paragraph("Idade",  hdr_style),
            Paragraph("Camisa", hdr_style),
            Paragraph("GC",     hdr_style),
            Paragraph("Setor",  hdr_style),
        ]]

        for a in abr.get(room["Id"], []):
            p = pm.get(a["ParticipantId"])
            if not p: continue
            age = age_from(p.get("BirthDate"))
            data.append([
                Paragraph(p["Name"],                                   cell_style),
                Paragraph(str(age) if age else "-",                    cell_style),
                Paragraph(SHIRT_MAP.get(p.get("ShirtSize", 0), "-"),  cell_style),
                Paragraph(p.get("ConnectionGroup")  or "-",            cell_style),
                Paragraph(p.get("ConnectionSector") or "-",            cell_style),
            ])

        t = Table(data, colWidths=COL_W, repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0), colors.HexColor("#F1F5F9")),
            ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",      (0, 0), (-1,-1), 8),
            ("GRID",          (0, 0), (-1,-1), 0.5, colors.HexColor("#CBD5E1")),
            ("VALIGN",        (0, 0), (-1,-1), "TOP"),
            ("TOPPADDING",    (0, 0), (-1,-1), 3),
            ("BOTTOMPADDING", (0, 0), (-1,-1), 3),
            ("LEFTPADDING",   (0, 0), (-1,-1), 3),
            ("RIGHTPADDING",  (0, 0), (-1,-1), 3),
        ]))
        elems.append(t)

    doc.build(elems)
    return buf.getvalue()

# ─── Letters / Photos helpers ─────────────────────────────────────────────────
def sheets_url_to_csv(url):
    if "export?format=csv" in url.lower(): return url
    m = re.search(r"spreadsheets/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m:
        sid = m.group(1)
        gm  = re.search(r"[?#&]gid=([0-9]+)", url, re.I)
        gid = gm.group(1) if gm else "0"
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&id={sid}&gid={gid}"
    return url

def fetch_sheet_csv(url):
    csv_url = sheets_url_to_csv(url)
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(csv_url, timeout=30, headers=headers)
        if resp.status_code == 200 and len(resp.text) > 10:
            return resp.text
    except: pass
    m = re.search(r"spreadsheets/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m:
        sid = m.group(1); gm = re.search(r"[?#&]gid=([0-9]+)", url, re.I)
        gid = gm.group(1) if gm else "0"
        gviz = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&gid={gid}"
        resp2 = requests.get(gviz, timeout=30, headers=headers)
        if resp2.status_code == 200:
            return resp2.text
    raise Exception("Não foi possível carregar a planilha. Verifique se está compartilhada.")

def extract_gdrive_id(url):
    if not url: return None
    m = re.search(r"drive\.google\.com/.*/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m: return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9\-_]+)", url, re.I)
    if m: return m.group(1)
    if "/" not in url and re.match(r"^[A-Za-z0-9\-_]+$", url): return url
    return None

def _do_load_letters(eid, url):
    try:
        text = fetch_sheet_csv(url)
        df = pd.read_csv(io.StringIO(text), dtype=str)
        df.columns = [c.strip() for c in df.columns]
        ct = find_header(df.columns, ["Para quem","Destinatário","Para","Nome do Encontrista","Encontrista","Nome do encontrista:","Nome do encontrista"])
        cf = find_header(df.columns, ["De quem","Remetente","De","Nome de quem escreve","Seu nome","Seu nome:"])
        cm = find_header(df.columns, ["Mensagem","Carta","Texto","Conteúdo","Escreva algo especial para ele(a) aqui:","Escreva algo especial para ele(a) aqui","Mensagem para ele(a)"])
        if not ct or not cm: return False, "Colunas não encontradas."
        letters = {}
        for _, row in df.iterrows():
            to = safe_str(row.get(ct))
            sender = safe_str(row.get(cf)) if cf else "—"
            if sender is None: sender = "—"
            msg = safe_str(row.get(cm)) or ""
            if to and msg: letters.setdefault(to, []).append({"sender": sender, "message": msg})
        total = sum(len(v) for v in letters.values())
        st.session_state[f"letters_dl_{eid}"] = {"ts": datetime.now().strftime("%H:%M:%S"), "total": total}
        st.session_state[f"letters_data_{eid}"] = letters
        return True, f"✅ {total} carta(s) carregadas"
    except Exception as e:
        return False, str(e)

def _do_load_photos(eid, url):
    try:
        text = fetch_sheet_csv(url)
        df = pd.read_csv(io.StringIO(text), dtype=str)
        df.columns = [c.strip() for c in df.columns]
        cn = find_header(df.columns, ["Nome do Encontrista", "Nome do encontrista:", "Nome do encontrista", "Encontrista", "Nome"])
        cp = find_header(df.columns, ["Foto", "Fotos", "URL da Foto", "URL das Fotos", "URL", "Link"])
        if not cn or not cp: return False, "Colunas não encontradas."
        groups = {}
        for _, row in df.iterrows():
            name = safe_str(row.get(cn)); photo = safe_str(row.get(cp))
            if name and photo:
                groups.setdefault(name, [])
                for lk in photo.split(","):
                    lk = lk.strip()
                    if lk: groups[name].append(lk)
        total = sum(len(v) for v in groups.values())
        st.session_state[f"photos_dl_{eid}"] = {"ts": datetime.now().strftime("%H:%M:%S"), "total": total}
        st.session_state[f"photo_groups_{eid}"] = groups
        st.session_state[f"photos_loaded_{eid}"] = True
        return True, f"✅ {total} foto(s) mapeadas"
    except Exception as e:
        return False, str(e)

def make_gdrive_view_url(raw_url):
    if not raw_url: return None
    raw_url = raw_url.strip(); fid = None
    m = re.search(r"[?&]id=([A-Za-z0-9\-_]+)", raw_url, re.I)
    if m: fid = m.group(1)
    if not fid:
        m = re.search(r"/d/([A-Za-z0-9\-_]+)", raw_url, re.I)
        if m: fid = m.group(1)
    if not fid and re.match(r"^[A-Za-z0-9\-_]{20,}$", raw_url): fid = raw_url
    if fid: return f"https://drive.google.com/file/d/{fid}/view"
    return raw_url

# ═══════════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════════════
def show_login():
    st.markdown("## 🔐 Gestão Encontro com Deus")
    st.caption("Informe a senha para acessar.")
    pwd = st.text_input("Senha", type="password", key="lp")
    if st.button("Entrar", type="primary", use_container_width=True):
        if pwd == st.secrets.get("APP_PASSWORD", "encontro2025"):
            st.session_state.authenticated = True
            st.query_params["auth"] = "1"
            st.rerun()
        else:
            st.error("Senha incorreta.")

def page_events():
    st.markdown("## ⛪ Eventos"); events = load_events()
    _, cr = st.columns([4, 1])
    with cr:
        if st.button("＋ Criar novo evento", type="primary", use_container_width=True): st.session_state.page = "event_new"; st.rerun()
    if not events: st.info("Nenhum evento criado ainda."); return
    for ev in events:
        with st.container(border=True):
            a, b, c = st.columns([4, 2, 1])
            with a: st.markdown(f"**{ev['Name']}**"); st.caption(f"{fmt_date_br(ev.get('StartDate'))} • {fmt_date_br(ev.get('EndDate'))}")
            with b:
                if st.button("Abrir Dashboard →", key=f"d_{ev['Id']}", use_container_width=True, type="primary"): 
                    st.session_state.current_event = ev["Id"]; st.session_state.page = "dashboard"; st.rerun()
            with c:
                if st.button("🗑️", key=f"x_{ev['Id']}"):
                    sb = get_sb()
                    for t in ["RoomAssignments","ScheduleItems","Rooms","Participants"]: sb.table(t).delete().eq("EventId", ev["Id"]).execute()
                    sb.table("Events").delete().eq("Id", ev["Id"]).execute(); st.rerun()

def page_event_new():
    st.markdown("## Novo Evento")
    if st.button("← Voltar"): st.session_state.page = "events"; st.rerun()
    with st.form("ne"):
        name = st.text_input("Nome do Evento", placeholder="Encontro com Deus - Abril")
        c1, c2 = st.columns(2)
        with c1: start = st.date_input("Data Início")
        with c2: end   = st.date_input("Data Fim")
        loc = st.text_input("Local (opcional)", placeholder="Ex.: Sítio Betel")
        if st.form_submit_button("Salvar e abrir Dashboard", type="primary"):
            if not name or len(name) < 3: st.error("Nome mín. 3 caracteres."); return
            nid = str(uuid.uuid4())
            get_sb().table("Events").insert({"Id": nid, "Name": name, "StartDate": start.isoformat(), "EndDate": end.isoformat(), "Location": loc or None, "Status": "Config", "CreatedAtUtc": utcnow()}).execute()
            st.session_state.current_event = nid; st.session_state.page = "dashboard"; st.rerun()

def event_sidebar(eid):
    ev = load_event(eid)
    if not ev: st.error("Evento não encontrado."); st.session_state.page = "events"; st.rerun(); return None
    with st.sidebar:
        st.markdown(f"### {ev['Name']}")
        if ev.get("StartDate"): st.caption(f"📅 {fmt_date_br(ev['StartDate'])} — {fmt_date_br(ev.get('EndDate'))}")
        st.divider()
        menu_items = {
            "dashboard":"📊 Dashboard",
            "participants":"👥 Participantes",
            "rooms":"🏠 Quartos",
            "secretary":"🗂️ Secretaria",
            "print_management":"🖨️ Gestão de Impressão",
            "settings":"⚙️ Configurações"
        }
        for k, label in menu_items.items():
            is_active = st.session_state.get("page") == k
            if st.button(label, key=f"n_{k}", use_container_width=True, type="primary" if is_active else "secondary"): 
                st.session_state.page = k; st.rerun()
        st.divider()
        if st.button("← Todos os eventos", use_container_width=True): st.session_state.pop("current_event", None); st.session_state.page = "events"; st.rerun()
    return ev

def page_dashboard(eid, ev):
    st.markdown(f"## 📊 Dashboard — {ev['Name']}")
    parts   = load_participants(eid)
    enc     = [p for p in parts if is_encounterist(p.get("Category"))]
    srv_com = [p for p in parts if is_server_with_shirt(p.get("Category"))]
    srv_sem = [p for p in parts if is_server_no_shirt(p.get("Category"))]
    equipe  = [p for p in parts if norm(p.get("Category","")).startswith("equipe")]
    
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Encontristas (M)", sum(1 for p in enc if p.get("Gender")==1))
    c2.metric("Encontristas (F)", sum(1 for p in enc if p.get("Gender")==2))
    c3.metric("Servos c/ camisa", len(srv_com)); c4.metric("Servos s/ camisa", len(srv_sem))
    c5,c6,c7,c8 = st.columns(4)
    c5.metric("Equipe", len(equipe)); c6.metric("Total Geral", len(parts)); c7.metric("Quartos", len(load_rooms(eid))); c8.metric("","")

    # ─── GRÁFICOS DE PIZZA E ATUALIZAÇÃO ───
    st.divider()
    col_title, col_btn = st.columns([3, 1])
    with col_title:
        st.markdown("### 📈 Progresso de Recebimentos")
    with col_btn:
        if st.button("🔄 Atualizar Dados Agora", type="primary", use_container_width=True):
            with st.spinner("Buscando no Google Drive..."):
                if ev.get("LettersSheetUrl"): _do_load_letters(eid, ev["LettersSheetUrl"])
                if ev.get("PhotosSheetUrl"): _do_load_photos(eid, ev["PhotosSheetUrl"])
                st.rerun()

    letters = st.session_state.get(f"letters_data_{eid}", {})
    photo_groups = st.session_state.get(f"photo_groups_{eid}", {})

    def count_letters(name):
        cnt = 0
        for key, lts in letters.items():
            if norm(key)==norm(name) or name.lower() in key.lower() or key.lower() in name.lower(): cnt += len(lts)
        return cnt

    def count_photos(name):
        for key, links in photo_groups.items():
            if norm(key)==norm(name) or name.lower() in key.lower() or key.lower() in name.lower(): return len(links)
        return 0

    total_enc = len(enc)
    if total_enc > 0:
        # Lógica exata: Varre cada encontrista e checa se ele tem cartas/fotos
        faltam_cartas = [p for p in enc if count_letters(p["Name"]) == 0]
        faltam_fotos = [p for p in enc if count_photos(p["Name"]) == 0]

        recebeu_carta = total_enc - len(faltam_cartas)
        nao_recebeu_carta = len(faltam_cartas)
        
        recebeu_foto = total_enc - len(faltam_fotos)
        nao_recebeu_foto = len(faltam_fotos)

        _, col_chart1, col_chart2, _ = st.columns([1, 4, 4, 1])
        
        try:
            import plotly.express as px
            with col_chart1:
                fig_cartas = px.pie(
                    names=["Com Cartas", "Faltam"], 
                    values=[recebeu_carta, nao_recebeu_carta],
                    title=f"Cartas (Base: {total_enc})",
                    color_discrete_sequence=["#10b981", "#e2e8f0"], 
                    hole=0.6 
                )
                fig_cartas.update_traces(textposition='inside', textinfo='percent+value', hoverinfo='label+percent')
                fig_cartas.update_layout(
                    height=280, 
                    showlegend=True, 
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                    margin=dict(t=30, b=0, l=0, r=0), 
                    paper_bgcolor='rgba(0,0,0,0)', 
                    plot_bgcolor='rgba(0,0,0,0)', 
                    font=dict(color='#1e293b')
                )
                st.plotly_chart(fig_cartas, use_container_width=True)

            with col_chart2:
                fig_fotos = px.pie(
                    names=["Com Fotos", "Faltam"], 
                    values=[recebeu_foto, nao_recebeu_foto],
                    title=f"Fotos (Base: {total_enc})",
                    color_discrete_sequence=["#3b82f6", "#e2e8f0"], 
                    hole=0.6
                )
                fig_fotos.update_traces(textposition='inside', textinfo='percent+value', hoverinfo='label+percent')
                fig_fotos.update_layout(
                    height=280,
                    showlegend=True, 
                    legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
                    margin=dict(t=30, b=0, l=0, r=0), 
                    paper_bgcolor='rgba(0,0,0,0)', 
                    plot_bgcolor='rgba(0,0,0,0)', 
                    font=dict(color='#1e293b')
                )
                st.plotly_chart(fig_fotos, use_container_width=True)
                
        except ImportError:
            import matplotlib.pyplot as plt
            with col_chart1:
                st.markdown(f"**Cartas (Base: {total_enc})**")
                fig1, ax1 = plt.subplots(figsize=(3, 3))
                fig1.patch.set_facecolor('#ffffff')
                ax1.pie([recebeu_carta, nao_recebeu_carta], labels=["Com Cartas", "Faltam"], autopct='%1.1f%%', 
                        startangle=90, colors=["#10b981", "#e2e8f0"], textprops={'color':"black"})
                centre_circle = plt.Circle((0,0),0.60,fc='white')
                fig1.gca().add_artist(centre_circle)
                ax1.axis('equal')
                st.pyplot(fig1)
                
            with col_chart2:
                st.markdown(f"**Fotos (Base: {total_enc})**")
                fig2, ax2 = plt.subplots(figsize=(3, 3))
                fig2.patch.set_facecolor('#ffffff')
                ax2.pie([recebeu_foto, nao_recebeu_foto], labels=["Com Fotos", "Faltam"], autopct='%1.1f%%', 
                        startangle=90, colors=["#3b82f6", "#e2e8f0"], textprops={'color':"black"})
                centre_circle = plt.Circle((0,0),0.60,fc='white')
                fig2.gca().add_artist(centre_circle)
                ax2.axis('equal')
                st.pyplot(fig2)

        # --- TABELAS DE QUEM FALTA ---
        st.divider()
        col_tab1, col_tab2 = st.columns(2)
        
        with col_tab1:
            st.markdown("🚨 **Encontristas SEM Cartas**")
            if faltam_cartas:
                df_c = pd.DataFrame([{"Nome do Encontrista": p["Name"], "GC": p.get("ConnectionGroup") or "-"} for p in faltam_cartas])
                st.dataframe(df_c, hide_index=True, use_container_width=True)
            else:
                st.success("Todos os encontristas já receberam cartas!")

        with col_tab2:
            st.markdown("🚨 **Encontristas SEM Fotos**")
            if faltam_fotos:
                df_f = pd.DataFrame([{"Nome do Encontrista": p["Name"], "GC": p.get("ConnectionGroup") or "-"} for p in faltam_fotos])
                st.dataframe(df_f, hide_index=True, use_container_width=True)
            else:
                st.success("Todos os encontristas já receberam fotos!")

    else:
        st.info("Nenhum encontrista cadastrado para gerar os gráficos.")

def page_participants(eid, ev):
    st.markdown(f"## 👥 Participantes — {ev['Name']}")
    with st.expander("📥 Importar CSV / ➕ Adicionar", expanded=False):
        uploaded = st.file_uploader("Arquivo CSV", type=["csv"], key="cu")
        replace  = st.checkbox("Substituir existentes", value=False)
        if st.button("Importar", type="primary") and uploaded:
            with st.spinner("Importando..."):
                ok, fail = import_csv(eid, uploaded.read(), replace); st.success(f"✅ {ok} importados, {fail} falha(s)."); st.rerun()
        st.divider()
        with st.form("ap"):
            a1,a2 = st.columns(2)
            with a1: pn=st.text_input("Nome*"); pg=st.selectbox("Gênero",["Masculino","Feminino","Não informado"]); pp=st.text_input("Telefone")
            with a2: pc=st.selectbox("Categoria",CATEGORY_OPTIONS); ps=st.selectbox("Camiseta",["-"]+SHIRT_KEYS); psc=st.text_input("Setor"); pgc=st.text_input("Grupo de Conexão")
            if st.form_submit_button("Adicionar Manualmente", type="primary"):
                if not pn: st.error("Nome obrigatório."); return
                get_sb().table("Participants").insert({"Id":str(uuid.uuid4()),"EventId":eid,"Name":pn,"Gender":GENDER_REV.get(pg,0),"ShirtSize":SHIRT_REV.get(ps,0),"Category":pc,"Phone":pp or None,"ConnectionSector":psc or None,"ConnectionGroup":pgc or None,"CreatedAtUtc":utcnow()}).execute()
                st.success(f"'{pn}' adicionado!"); st.rerun()

    parts  = load_participants(eid); assigns = load_assignments(eid); rooms = load_rooms(eid)
    rm = {r["Id"]: r["Name"] for r in rooms}; pr = {a["ParticipantId"]: rm.get(a["RoomId"],"-") for a in assigns}

    fc1,fc2,fc3,fc4 = st.columns(4)
    with fc1: search = st.text_input("🔍 Buscar Nome", key="ps")
    with fc2: fg     = st.selectbox("Sexo", ["Todos","Masculino","Feminino"], key="fg")
    with fc3: fc_sel = st.selectbox("Categoria", ["Todas","Encontrista","Servo","Equipe"], key="fc")
    with fc4: fsc    = st.text_input("Setor", key="fsc")
    
    filtered = parts
    if search:       filtered = [p for p in filtered if search.lower() in p["Name"].lower()]
    if fg=="Masculino":  filtered = [p for p in filtered if p.get("Gender")==1]
    elif fg=="Feminino": filtered = [p for p in filtered if p.get("Gender")==2]
    if fc_sel=="Encontrista": filtered = [p for p in filtered if is_encounterist(p.get("Category"))]
    elif fc_sel=="Servo":     filtered = [p for p in filtered if is_server(p.get("Category"))]
    elif fc_sel=="Equipe":    filtered = [p for p in filtered if norm(p.get("Category","")).startswith("equipe")]
    if fsc: filtered = [p for p in filtered if fsc.lower() in norm(p.get("ConnectionSector",""))]

    st.markdown(f"**{len(filtered)} participante(s)**")

    ac1, ac2 = st.columns(2)
    with ac1:
        if filtered:
            pdf_data = generate_sector_pdf(filtered, ev_name=ev["Name"], assigns=assigns, rooms=rooms)
            st.download_button("⬇️ Baixar PDF Setores", data=pdf_data, file_name="setores.pdf", mime="application/pdf", use_container_width=True)
    with ac2:
        if filtered:
            rows_exp = [{"Nome": p["Name"], "Categoria": p.get("Category") or "-", "Sexo": GENDER_MAP.get(p.get("Gender",0),"-"), "Camisa": SHIRT_MAP.get(p.get("ShirtSize",0),"-"), "Quarto": pr.get(p["Id"],"-"), "Setor": p.get("ConnectionSector") or "-"} for p in filtered]
            csv_data = pd.DataFrame(rows_exp).to_csv(index=False).encode('utf-8')
            st.download_button("⬇️ Baixar CSV", data=csv_data, file_name="participantes.csv", mime="text/csv", use_container_width=True)

    st.divider()
    if not filtered: st.info("Nenhum participante encontrado.")
    else:
        for p in filtered:
            pid = p["Id"]; edit_key = f"pedit_{pid}"
            quarto = pr.get(pid, "-")
            with st.container(border=True):
                c1, c2, c3 = st.columns([6, 1, 1])
                with c1:
                    st.markdown(f"**{p['Name']}** · {GENDER_MAP.get(p.get('Gender',0),'-')} · Camisa: {SHIRT_MAP.get(p.get('ShirtSize',0),'-')}")
                    st.caption(f"{p.get('Category') or '-'} · Quarto: {quarto} · Setor: {p.get('ConnectionSector') or '-'}")
                with c2:
                    if st.button("✏️ Editar", key=f"ebtn_{pid}", use_container_width=True): st.session_state[edit_key] = not st.session_state.get(edit_key, False); st.rerun()
                with c3:
                    if st.button("🗑️ Apagar", key=f"delbtn_{pid}", use_container_width=True):
                        sb2 = get_sb(); sb2.table("RoomAssignments").delete().eq("ParticipantId", pid).execute(); sb2.table("Participants").delete().eq("Id", pid).execute(); st.rerun()
                
                if st.session_state.get(edit_key, False):
                    with st.form(f"pform_{pid}"):
                        ea, eb = st.columns(2)
                        with ea:
                            en    = st.text_input("Nome", value=p.get("Name",""))
                            eg    = st.selectbox("Gênero", ["Masculino","Feminino","Não informado"], index=max(0, p.get("Gender",0)-1) if p.get("Gender",0)>0 else 2)
                            ecat  = st.selectbox("Categoria", CATEGORY_OPTIONS, index=CATEGORY_OPTIONS.index(p["Category"]) if p.get("Category") in CATEGORY_OPTIONS else 0)
                        with eb:
                            esh  = st.selectbox("Camisa", ["-"]+SHIRT_KEYS, index=(["-"]+SHIRT_KEYS).index(SHIRT_MAP.get(p.get("ShirtSize",0),"-")))
                            esc  = st.text_input("Setor", value=p.get("ConnectionSector") or "")
                            egc  = st.text_input("GC", value=p.get("ConnectionGroup") or "")
                        if st.form_submit_button("💾 Salvar Modificações", type="primary"):
                            get_sb().table("Participants").update({"Name": en, "Gender": GENDER_REV.get(eg, 0), "ShirtSize": SHIRT_REV.get(esh, 0), "Category": ecat, "ConnectionSector": esc or None, "ConnectionGroup": egc or None, "UpdatedAtUtc": utcnow()}).eq("Id", pid).execute()
                            st.session_state.pop(edit_key, None); st.rerun()

def page_rooms(eid, ev):
    st.markdown(f"## 🏠 Quartos — {ev['Name']}")
    parts  = load_participants(eid); rooms = load_rooms(eid); assigns = load_assignments(eid)
    pm     = {p["Id"]: p for p in parts}; sb2 = get_sb()

    b1, b2, b3 = st.columns([1, 1, 2])
    with b1:
        if st.button("🔀 Auto-Distribuir", type="primary", use_container_width=True):
            with st.spinner("Distribuindo..."):
                n, nr, err = distribute_rooms(eid)
                if err: st.error(err)
                else: st.success(f"✅ {n} alocados!"); st.rerun()
    with b2:
        if rooms:
            pdf_r = generate_rooms_pdf(eid)
            st.download_button("⬇️ Baixar PDF Quartos", data=pdf_r, file_name="quartos.pdf", mime="application/pdf", use_container_width=True)

    with st.expander("➕ Novo Quarto", expanded=False):
        with st.form("nr"):
            rc1,rc2,rc3 = st.columns(3)
            with rc1: rn   = st.text_input("Nome")
            with rc2: rcap = st.number_input("Capacidade", min_value=1, value=10)
            with rc3: rg   = st.selectbox("Gênero", ["Feminino","Masculino"])
            if st.form_submit_button("Criar Quarto", type="primary"):
                if rn:
                    sb2.table("Rooms").insert({"Id":str(uuid.uuid4()),"EventId":eid,"Name":rn,"Capacity":rcap,"Gender":2 if rg=="Feminino" else 1,"CreatedAtUtc":utcnow()}).execute(); st.rerun()

    st.divider()
    if not rooms: st.info("Nenhum quarto cadastrado."); return

    am = {}; aids = set()
    for a in assigns: 
        am.setdefault(a["RoomId"], []).append(a)
        aids.add(a["ParticipantId"])

    for room in rooms:
        rid = room["Id"]; occs = am.get(rid, [])
        gender_str = GENDER_MAP.get(room.get('Gender', 0), '-')
        
        with st.expander(f"🏠 {room['Name']} ({gender_str}) — {len(occs)}/{room['Capacity']} vagas", expanded=False):
            col_ed1, col_ed2 = st.columns([3, 1])
            with col_ed1:
                with st.popover("⚙️ Editar Dados do Quarto"):
                    new_rn = st.text_input("Nome", value=room['Name'], key=f"rn_{rid}")
                    new_cap = st.number_input("Capacidade", value=room['Capacity'], min_value=1, key=f"rc_{rid}")
                    new_g_str = "Masculino" if room.get('Gender')==1 else "Feminino"
                    new_g = st.selectbox("Gênero", ["Feminino","Masculino"], index=["Feminino","Masculino"].index(new_g_str), key=f"rg_{rid}")
                    if st.button("💾 Salvar Alterações", type="primary", key=f"sroom_{rid}"):
                        sb2.table("Rooms").update({"Name":new_rn, "Capacity":new_cap, "Gender":2 if new_g=="Feminino" else 1}).eq("Id",rid).execute(); st.rerun()
            with col_ed2:
                if st.button("🗑️ Apagar Quarto", key=f"delr_{rid}", use_container_width=True):
                    sb2.table("RoomAssignments").delete().eq("RoomId",rid).execute(); sb2.table("Rooms").delete().eq("Id",rid).execute(); st.rerun()

            st.divider()

            if occs:
                for a in occs:
                    p = pm.get(a["ParticipantId"])
                    if not p: continue
                    o1, o2, o3 = st.columns([5, 3, 1])
                    with o1: 
                        st.markdown(f"👤 **{p['Name']}**")
                    with o2:
                        other_rooms = [r for r in rooms if r["Id"] != rid and (r.get("Gender",0)==p.get("Gender",0) or r.get("Gender",0)==0)]
                        if other_rooms:
                            with st.popover("🔄 Trocar/Mover"):
                                dest_room_name = st.selectbox("Mover para:", [r["Name"] for r in other_rooms], key=f"dest_{a['Id']}")
                                dest_room = next(r for r in other_rooms if r["Name"]==dest_room_name)
                                dest_occs = am.get(dest_room["Id"], [])
                                
                                if len(dest_occs) >= dest_room["Capacity"]:
                                    st.warning("O quarto destino está cheio. Com quem você quer trocar?")
                                    swap_candidates = [pm.get(da["ParticipantId"]) for da in dest_occs if pm.get(da["ParticipantId"])]
                                    swap_p = st.selectbox("Trocar lugar com:", [c["Name"] for c in swap_candidates], key=f"swapc_{a['Id']}")
                                    if st.button("Executar Troca", type="primary", key=f"bswap_{a['Id']}"):
                                        try:
                                            asgn_a_id = a["Id"]
                                            target_pid = next((c["Id"] for c in swap_candidates if c["Name"] == swap_p), None)
                                            target_asgn = next((da for da in dest_occs if da["ParticipantId"] == target_pid), None)
                                            
                                            if target_asgn:
                                                asgn_b_id = target_asgn["Id"]
                                                sb2.table("RoomAssignments").update({"RoomId": rid}).eq("Id", asgn_b_id).execute()
                                                sb2.table("RoomAssignments").update({"RoomId": dest_room["Id"]}).eq("Id", asgn_a_id).execute()
                                                st.rerun()
                                        except Exception as e:
                                            st.error(f"Erro ao realizar troca: {e}")
                                else:
                                    if st.button("Mover para Quarto", type="primary", key=f"bmove_{a['Id']}"):
                                        try:
                                            sb2.table("RoomAssignments").update({"RoomId": dest_room["Id"]}).eq("Id", a["Id"]).execute()
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Erro ao mover: {e}")
                    with o3:
                        if st.button("✕ Remover", key=f"rem_{a['Id']}"):
                            sb2.table("RoomAssignments").delete().eq("Id",a["Id"]).execute(); st.rerun()
            
            unass = [p for p in parts if p["Id"] not in aids and is_encounterist(p.get("Category")) and (p.get("Gender",0)==room.get("Gender",0) or room.get("Gender",0)==0)]
            if unass and len(occs) < room["Capacity"]:
                sp = st.selectbox("Adicionar pessoa sem quarto:", [""]+[p["Name"] for p in unass], key=f"addp_{rid}")
                if sp and st.button("＋ Adicionar", type="primary", key=f"badd_{rid}"):
                    pid = next(p["Id"] for p in unass if p["Name"]==sp)
                    sb2.table("RoomAssignments").insert({"Id":str(uuid.uuid4()),"EventId":eid,"RoomId":rid,"ParticipantId":pid,"CreatedAtUtc":utcnow()}).execute(); st.rerun()


# ─── MÓDULO CENTRAL DE IMPRESSÃO ───────────────────────────────
def page_print_management(eid, ev):
    st.markdown(f"## 🖨️ Gestão de Impressão — {ev['Name']}")
    st.info("⚠️ **Atenção (Letícia/Equipe de Impressão):** Ao imprimir, vá imediatamente na planilha/formulário do Google e **remova o nome do encontrista** para travar o recebimento de novas cartas ou fotos.")
    
    parts = load_participants(eid)
    _, _, sec_status = load_secretary_state(eid)
    
    print_queue = [p for p in parts if sec_status.get(p["Id"], {}).get("print_status") in ["requested", "printing"]]
    
    if not print_queue:
        st.success("Nenhuma solicitação pendente. A secretaria envia solicitações para esta tela.")
        return

    def mark_as_printing(pid):
        sec_status[pid]["print_status"] = "printing"
        save_secretary_state(eid, *load_secretary_state(eid)[:2], sec_status)

    for p in print_queue:
        pid = p["Id"]
        status = sec_status[pid]["print_status"]
        req_by = sec_status[pid].get("print_req_by", "Secretaria")
        
        with st.container(border=True):
            c1, c2, c3 = st.columns([4, 2, 2])
            with c1:
                st.markdown(f"### {p['Name']}")
                st.caption(f"Solicitante: **{req_by}** · Status: **{'🟡 Aguardando' if status == 'requested' else '🖨️ Em Andamento (Notificado)'}**")
            
            letters_dict = st.session_state.get(f"letters_data_{eid}", {})
            user_letters = []
            for key, lts in letters_dict.items():
                if norm(key) == norm(p["Name"]) or p["Name"].lower() in key.lower() or key.lower() in p["Name"].lower():
                    user_letters.extend(lts)
            
            with c2:
                if user_letters:
                    docx_bytes = generate_letters_docx(p["Name"], user_letters)
                    st.download_button(
                        label=f"⬇️ Baixar Cartas ({len(user_letters)})", 
                        data=docx_bytes, 
                        file_name=f"Cartas_{norm(p['Name'])}.docx",
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        on_click=mark_as_printing, args=(pid,),
                        use_container_width=True, type="primary" if status=="requested" else "secondary"
                    )
                else:
                    st.button("Sem cartas", disabled=True, use_container_width=True)

            with c3:
                photo_groups = st.session_state.get(f"photo_groups_{eid}", {})
                user_photos = []
                for key, lks in photo_groups.items():
                    if norm(key) == norm(p["Name"]) or p["Name"].lower() in key.lower() or key.lower() in p["Name"].lower():
                        user_photos.extend(lks)
                
                if user_photos:
                    links_txt = f"FOTOS: {p['Name']}\n\n" + "\n".join([make_gdrive_view_url(lk) or lk for lk in user_photos])
                    st.download_button(
                        label=f"⬇️ Baixar Links Fotos ({len(user_photos)})", 
                        data=links_txt.encode('utf-8'), 
                        file_name=f"Fotos_{norm(p['Name'])}.txt",
                        mime="text/plain",
                        on_click=mark_as_printing, args=(pid,),
                        use_container_width=True, type="primary" if status=="requested" else "secondary"
                    )
                else:
                    st.button("Sem fotos", disabled=True, use_container_width=True)
            
            if status == "printing":
                st.divider()
                if st.button("✅ Concluir Impressão (Remover da Fila)", key=f"fin_{pid}", type="primary"):
                    sec_status[pid]["print_status"] = "done"
                    save_secretary_state(eid, *load_secretary_state(eid)[:2], sec_status)
                    st.rerun()

def generate_letters_docx(participant_name, letters_list):
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    doc = Document()
    
    for section in doc.sections:
        section.top_margin = Inches(1)
        section.bottom_margin = Inches(1)
        section.left_margin = Inches(1.2)
        section.right_margin = Inches(1.2)
    
    for i, carta in enumerate(letters_list):
        if i > 0: 
            doc.add_page_break()
        
        h = doc.add_paragraph()
        h.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run_h = h.add_run(f"Para: {participant_name}")
        run_h.bold = True
        run_h.font.size = Pt(14)
        doc.add_paragraph()
        
        sender_p = doc.add_paragraph()
        run_s = sender_p.add_run(f"De: {carta.get('sender','—')}")
        run_s.bold = True
        run_s.font.size = Pt(11)
        doc.add_paragraph()
        
        msg_p = doc.add_paragraph(carta.get("message",""))
        if msg_p.runs: 
            msg_p.runs[0].font.size = Pt(11)

    buf = io.BytesIO()
    doc.save(buf)
    buf.seek(0)
    return buf.getvalue()

def page_secretary(eid, ev):
    st.markdown(f"## 🗂️ Secretaria — {ev['Name']}")
    parts  = load_participants(eid); assigns = load_assignments(eid); rooms = load_rooms(eid)
    enc    = [p for p in parts if is_encounterist(p.get("Category"))]
    rm     = {r["Id"]: r["Name"] for r in rooms}; pr = {a["ParticipantId"]: rm.get(a["RoomId"], "-") for a in assigns}
    
    db_team, db_dist, db_status = load_secretary_state(eid)
    team = st.session_state.get(f"sec_team_{eid}", db_team)
    dist = st.session_state.get(f"sec_dist_{eid}", db_dist)
    sec_status = st.session_state.get(f"sec_status_{eid}", db_status)

    def persist(): 
        save_secretary_state(eid, team, dist, sec_status)
        st.session_state[f"sec_team_{eid}"] = team
        st.session_state[f"sec_dist_{eid}"] = dist
        st.session_state[f"sec_status_{eid}"] = sec_status

    letters = st.session_state.get(f"letters_data_{eid}", {})
    photo_groups = st.session_state.get(f"photo_groups_{eid}", {})

    def count_letters(name):
        cnt = 0
        for key, lts in letters.items():
            if norm(key)==norm(name) or name.lower() in key.lower() or key.lower() in name.lower(): cnt += len(lts)
        return cnt

    def count_photos(name):
        for key, links in photo_groups.items():
            if norm(key)==norm(name) or name.lower() in key.lower() or key.lower() in name.lower(): return len(links)
        return 0

    tab1, tab2 = st.tabs(["📋 Acompanhamento das Bolsas", "⚙️ Distribuição de Equipe"])
    
    with tab1:
        st.markdown("### 🔄 Atualização de Recebimentos")
        col_ref1, col_ref2 = st.columns([3, 4])
        with col_ref1:
            if st.button("Buscar Novas Cartas/Fotos", type="primary", use_container_width=True):
                with st.spinner("Sincronizando com o Google Drive..."):
                    if ev.get("LettersSheetUrl"): _do_load_letters(eid, ev["LettersSheetUrl"])
                    if ev.get("PhotosSheetUrl"): _do_load_photos(eid, ev["PhotosSheetUrl"])
                    st.rerun()
        with col_ref2:
            dl_l = st.session_state.get(f"letters_dl_{eid}", {})
            dl_p = st.session_state.get(f"photos_dl_{eid}", {})
            st.info(f"**Última Sincronização:**\nCartas: {dl_l.get('ts','Nunca')} | Fotos: {dl_p.get('ts','Nunca')}")

        st.divider()
        if not dist:
            st.warning("Vá na aba 'Distribuição de Equipe' para atribuir as bolsas aos responsáveis.")
        else:
            membro_sel = st.selectbox("👤 Ver acompanhamento de:", list(dist.keys()), key="sec_member_sel")
            pids_sel = dist.get(membro_sel, []); people_sel = [p for p in enc if p["Id"] in pids_sel]
            
            for p in people_sel:
                pid = p["Id"]; ps = sec_status.get(pid, {})
                bolsa_ok = ps.get("bolsa_ok", False); cartas_ok = ps.get("cartas_ok", False); fotos_ok = ps.get("fotos_ok", False)
                print_stat = ps.get("print_status", "none")
                n_cartas = count_letters(p["Name"]); n_fotos = count_photos(p["Name"])
                quarto = pr.get(pid, "-")
                
                with st.container(border=True):
                    r1, r2, r3 = st.columns([3, 2, 2])
                    with r1:
                        st.markdown(f"**{p['Name']}**")
                        st.caption(f"Quarto: {quarto}")
                    with r2:
                        st.markdown(f"💌 **Cartas:** {n_cartas} | 📷 **Fotos:** {n_fotos}")
                        if not bolsa_ok:
                            c1, c2 = st.columns(2)
                            with c1:
                                if st.checkbox("Cartas OK", value=cartas_ok, key=f"ck_{pid}") != cartas_ok:
                                    if pid not in sec_status: sec_status[pid] = {}
                                    sec_status[pid]["cartas_ok"] = not cartas_ok; persist(); st.rerun()
                            with c2:
                                if st.checkbox("Fotos OK", value=fotos_ok, key=f"fk_{pid}") != fotos_ok:
                                    if pid not in sec_status: sec_status[pid] = {}
                                    sec_status[pid]["fotos_ok"] = not fotos_ok; persist(); st.rerun()
                    with r3:
                        if bolsa_ok:
                            st.success("✅ Bolsa Finalizada")
                            if st.button("↩️ Reabrir Bolsa", key=f"reopen_{pid}"):
                                sec_status[pid]["bolsa_ok"] = False; persist(); st.rerun()
                        else:
                            if print_stat == "requested":
                                st.warning("🖨️ Aguardando Impressão")
                            elif print_stat == "printing":
                                st.info("🖨️ Sendo impresso (Notificado)")
                            elif print_stat == "done":
                                if st.button("✅ Fechar Bolsa", type="primary", key=f"done_{pid}", use_container_width=True):
                                    sec_status[pid]["bolsa_ok"] = True; persist(); st.rerun()
                            else:
                                if st.button("🖨️ Solicitar Impressão", key=f"reqprint_{pid}", use_container_width=True):
                                    if pid not in sec_status: sec_status[pid] = {}
                                    sec_status[pid]["print_status"] = "requested"
                                    sec_status[pid]["print_req_by"] = membro_sel
                                    persist(); st.rerun()

    with tab2:
        st.markdown("#### Equipe Responsável pelas Bolsas")
        servos = [p for p in parts if is_server(p.get("Category"))]; servo_names = [p["Name"] for p in servos]
        novo_membro = st.selectbox("Selecionar membro", [""] + servo_names, key="sec_sel_servo")
        if st.button("➕ Adicionar membro à equipe") and novo_membro:
            if novo_membro not in team: team.append(novo_membro); persist(); st.rerun()
            
        st.write(f"Membros cadastrados: {', '.join(team) if team else 'Nenhum'}")
        st.divider()
        if st.button("🔀 Distribuir encontristas automaticamente (Divisão igualitária)", type="primary"):
            by_room = {}
            for p in enc: quarto = pr.get(p["Id"], "Sem quarto"); by_room.setdefault(quarto, []).append(p)
            ordered = [item for sublist in [by_room[k] for k in sorted(by_room.keys())] for item in sublist]
            new_dist = {m: [] for m in team}
            for idx, p in enumerate(ordered): new_dist[team[idx % len(team)]].append(p["Id"])
            dist = new_dist; persist(); st.success("✅ Distribuição feita com sucesso!"); st.rerun()

def page_settings(eid, ev):
    st.markdown(f"## ⚙️ Configurações — {ev['Name']}")
    with st.form("cfg"):
        name = st.text_input("Nome do Evento", value=ev.get("Name",""))
        lu = st.text_input("Link da Planilha de Cartas (Google Sheets)", value=ev.get("LettersSheetUrl") or "")
        phu = st.text_input("Link da Planilha de Fotos (Google Sheets)", value=ev.get("PhotosSheetUrl") or "")
        st.caption("Atenção: Garanta que o link seja de compartilhamento 'Qualquer pessoa com o link pode ler'")
        if st.form_submit_button("💾 Salvar Configurações", type="primary"):
            get_sb().table("Events").update({"Name":name, "LettersSheetUrl":lu or None, "PhotosSheetUrl":phu or None, "UpdatedAtUtc":utcnow()}).eq("Id",eid).execute()
            st.success("Configurações salvas!"); st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(page_title="Gestão Encontro com Deus", page_icon="⛪", layout="wide", initial_sidebar_state="expanded")
    inject_custom_css()

    if "authenticated" not in st.session_state: st.session_state.authenticated = False
    if not st.session_state.authenticated and st.query_params.get("auth") == "1": st.session_state.authenticated = True

    if not st.session_state.authenticated: show_login(); return

    with st.sidebar:
        st.markdown("### ⛪ Encontro com Deus"); st.divider()
        if st.button("🚪 Sair", use_container_width=True):
            st.session_state.authenticated = False; st.query_params.clear(); st.session_state.pop("current_event", None); st.rerun()

    if "page" not in st.session_state: st.session_state.page = "events"
    page = st.session_state.page; eid = st.session_state.get("current_event")
    
    if page == "event_new": page_event_new(); return
    if page == "events" or not eid: page_events(); return
    
    ev = event_sidebar(eid)
    if not ev: return
    
    routes = {
        "dashboard": page_dashboard,
        "participants": page_participants,
        "rooms": page_rooms,
        "secretary": page_secretary,
        "print_management": page_print_management,
        "settings": page_settings
    }
    
    routes.get(page, page_dashboard)(eid, ev)

if __name__ == "__main__": main()
