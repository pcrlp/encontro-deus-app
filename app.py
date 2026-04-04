"""
Gestão Encontro com Deus — Streamlit + Supabase
"""
import streamlit as st
import pandas as pd
import uuid, io, re, math, requests, zipfile, unicodedata
from datetime import datetime, date, timezone
from typing import Optional
from supabase import create_client, Client

# ─── Config & Supabase ───────────────────────────────────────────────────────
def get_sb() -> Client:
    return create_client(st.secrets["SUPABASE_URL"], st.secrets["SUPABASE_SERVICE_KEY"])

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

# ─── Enums (idênticos ao .NET) ────────────────────────────────────────────────
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
        return abs(age)  # sempre positivo
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

def time_to_minutes(t):
    """Converte 'HH:MM' em minutos. Retorna None se inválido."""
    if not t: return None
    try:
        parts = str(t).strip().split(":")
        return int(parts[0]) * 60 + int(parts[1])
    except: return None

# ─── Data loaders ─────────────────────────────────────────────────────────────
def load_events(): return get_sb().table("Events").select("*").order("CreatedAtUtc", desc=True).execute().data or []
def load_event(eid):
    r = get_sb().table("Events").select("*").eq("Id", eid).execute(); return r.data[0] if r.data else None
def load_participants(eid): return get_sb().table("Participants").select("*").eq("EventId", eid).order("Name").execute().data or []
def load_rooms(eid): return get_sb().table("Rooms").select("*").eq("EventId", eid).order("Name").execute().data or []
def load_assignments(eid): return get_sb().table("RoomAssignments").select("*").eq("EventId", eid).execute().data or []
def load_schedule(eid): return get_sb().table("ScheduleItems").select("*").eq("EventId", eid).order("Day").order("Order").execute().data or []

def save_secretary_state(eid, team, dist, sec_status):
    """Persiste estado da secretária no campo SecretaryState do evento."""
    import json
    payload = json.dumps({"team": team, "dist": dist, "status": sec_status})
    try: get_sb().table("Events").update({"SecretaryState": payload, "UpdatedAtUtc": utcnow()}).eq("Id", eid).execute()
    except: pass  # coluna pode não existir ainda

def load_secretary_state(eid):
    """Carrega estado persistido da secretária."""
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

    col_name = find_header(df.columns, ["Nome do Encontrista", "Encontrista", "Nome", "Participante"])
    col_cat = find_header(df.columns, ["Categoria"])
    col_email = find_header(df.columns, ["Email", "E-mail"])
    col_gender = find_header(df.columns, ["Sexo"])
    col_shirt = find_header(df.columns, ["Tamanho da Camisa", "Tamanho da Camisa ", "Camiseta", "Camisa"])
    col_birth = find_header(df.columns, ["Data de Nascimento", "Nascimento"])
    col_phone = find_header(df.columns, ["Celular", "Telefone", "Celular (WhatsApp)", "Celular/WhatsApp", "Telefone Celular", "Celular / WhatsApp", "Contato (celular)"])
    col_marital = find_header(df.columns, ["Estado civil", "Estado Civil"])
    col_sector = find_header(df.columns, ["Qual nome do setor do grupo de conexão?", "Qual o nome do setor do grupo de conexão?", "Setor de Conexão", "Setor de Conexao", "Setor (Centro, Norte...)", "Setor"])
    col_group = find_header(df.columns, ["Nome do Grupo de Conexão", "Se a sua resposta anterior foi afirmativa, qual é o nome do seu GC?", "Qual é o nome do seu GC", "nome do seu gc", "Grupo de Conexão", "Grupo de Conexao", "Nome do GC", "GC"])
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
        rooms = sorted([r for r in all_rooms if r.get("Gender") == gv], key=lambda r: r["Capacity"])
        people = [p for p in enc if p.get("Gender") == gv]
        if not rooms or not people: return []
        seniors = sorted([p for p in people if (age_from(p.get("BirthDate")) or 0) >= 50], key=lambda p: -(age_from(p.get("BirthDate")) or 0))
        others = sorted([p for p in people if (age_from(p.get("BirthDate")) or 0) < 50], key=lambda p: -(age_from(p.get("BirthDate")) or 0))
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
    from reportlab.lib.pagesizes import A4; from reportlab.lib import colors
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
    styles = getSampleStyleSheet()
    cell_style = ParagraphStyle("cell", fontName="Helvetica", fontSize=8, leading=10)
    hdr_style  = ParagraphStyle("hdr",  fontName="Helvetica-Bold", fontSize=8, leading=10)
    title_style = ParagraphStyle("title2", fontName="Helvetica-Bold", fontSize=13, leading=16, spaceAfter=4)
    sub_style   = ParagraphStyle("sub",   fontName="Helvetica", fontSize=9, leading=11, textColor=colors.HexColor("#555555"))

    def build_section(group_label, group_parts):
        """Retorna lista de elementos platypus para uma seção."""
        elems = []
        elems.append(Paragraph(ev_name, title_style))
        elems.append(Paragraph(f"{group_label} — {len(group_parts)} pessoa(s)", sub_style))
        elems.append(Paragraph(f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}", sub_style))
        elems.append(Spacer(1, 10))

        sorted_parts = sorted(group_parts, key=lambda p: (
            ORDER.index(sk(p.get("ConnectionSector", ""))), p.get("Name", "")
        ))

        data = [[
            Paragraph("Nº", hdr_style),
            Paragraph("Nome", hdr_style),
            Paragraph("Categoria", hdr_style),
            Paragraph("Quarto", hdr_style),
            Paragraph("Setor / GC", hdr_style),
            Paragraph("Convidado por", hdr_style),
        ]]
        row_colors = []

        for i, p in enumerate(sorted_parts):
            sector = p.get("ConnectionSector") or ""
            gc = p.get("ConnectionGroup") or "-"
            setor_gc = f"{sector or '-'} / {gc}" if sector else gc
            quarto = pr.get(p["Id"], "-")
            invited = p.get("InvitedBy") or "-"
            cat = p.get("Category") or "-"
            data.append([
                Paragraph(str(i+1), cell_style),
                Paragraph(p.get("Name", ""), cell_style),
                Paragraph(cat, cell_style),
                Paragraph(quarto, cell_style),
                Paragraph(setor_gc, cell_style),
                Paragraph(invited, cell_style),
            ])
            row_colors.append(colors.HexColor(SC.get(sk(sector), "#F9FAFB")))

        col_widths = [22, 145, 75, 65, 110, 110]
        t = Table(data, colWidths=col_widths, repeatRows=1)
        table_style = [
            ("BACKGROUND", (0,0), (-1,0), colors.HexColor("#E2E8F0")),
            ("GRID", (0,0), (-1,-1), 0.4, colors.HexColor("#CBD5E1")),
            ("VALIGN", (0,0), (-1,-1), "TOP"),
            ("TOPPADDING", (0,0), (-1,-1), 3),
            ("BOTTOMPADDING", (0,0), (-1,-1), 3),
            ("LEFTPADDING", (0,0), (-1,-1), 3),
            ("RIGHTPADDING", (0,0), (-1,-1), 3),
        ]
        for i, c in enumerate(row_colors):
            table_style.append(("BACKGROUND", (0, i+1), (-1, i+1), c))
        t.setStyle(TableStyle(table_style))
        elems.append(t)
        return elems

    # ── Separa grupos ────────────────────────────────────────────────────────
    encontristas = [p for p in participants if is_encounterist(p.get("Category"))]
    servos       = [p for p in participants if is_server(p.get("Category"))]
    equipe       = [p for p in participants if norm(p.get("Category","")).startswith("equipe")]
    outros       = [p for p in participants if not is_encounterist(p.get("Category")) and not is_server(p.get("Category")) and not norm(p.get("Category","")).startswith("equipe")]

    all_elems = []
    sections = []
    if encontristas: sections.append(("Encontristas", encontristas))
    if servos:       sections.append(("Servos", servos))
    if equipe:       sections.append(("Equipe", equipe))
    if outros:       sections.append(("Outros", outros))

    # Se não há nenhuma das categorias conhecidas, mostra tudo junto
    if not sections:
        sections = [("Participantes", participants)]

    for idx, (label, grp) in enumerate(sections):
        if idx > 0:
            all_elems.append(PageBreak())
        all_elems.extend(build_section(label, grp))

    doc.build(all_elems)
    return buf.getvalue()

def generate_rooms_pdf(event_id):
    from reportlab.lib.pagesizes import A4; from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet
    rooms = load_rooms(event_id); assigns = load_assignments(event_id); parts = load_participants(event_id)
    pm = {p["Id"]: p for p in parts}; abr = {}
    for a in assigns: abr.setdefault(a["RoomId"], []).append(a)
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet(); elems = []
    for ri, room in enumerate(rooms):
        if ri > 0: elems.append(PageBreak())
        leader = pm.get(room.get("LeaderId")) if room.get("LeaderId") else None
        elems.append(Paragraph(f"Quarto: {room['Name']}", styles["Title"]))
        elems.append(Paragraph(f"Sexo: {GENDER_MAP.get(room.get('Gender',0),'-')} · Capacidade: {room['Capacity']} · Líder: {leader['Name'] if leader else '-'}", styles["Normal"]))
        elems.append(Spacer(1, 12))
        data = [["Nome", "Idade", "Camisa", "GC", "Setor"]]
        for a in abr.get(room["Id"], []):
            p = pm.get(a["ParticipantId"])
            if not p: continue
            age = age_from(p.get("BirthDate"))
            data.append([p["Name"], str(age) if age else "-", SHIRT_MAP.get(p.get("ShirtSize",0),"-"), p.get("ConnectionGroup") or "-", p.get("ConnectionSector") or "-"])
        t = Table(data, colWidths=[180, 40, 45, 120, 80])
        t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#F1F5F9")),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),9),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#CBD5E1")),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3)]))
        elems.append(t)
    doc.build(elems); return buf.getvalue()

# ─── Letters / Photos helpers ─────────────────────────────────────────────────
def sheets_url_to_csv(url):
    if "export?format=csv" in url.lower(): return url
    m = re.search(r"spreadsheets/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m:
        sid = m.group(1)
        gm = re.search(r"[?#&]gid=([0-9]+)", url, re.I)
        gid = gm.group(1) if gm else "0"
        return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&id={sid}&gid={gid}"
    return url

def fetch_sheet_csv(url):
    """Tenta buscar o CSV da planilha com fallback para gviz."""
    csv_url = sheets_url_to_csv(url)
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(csv_url, timeout=30, headers=headers)
        if resp.status_code == 200 and len(resp.text) > 10:
            return resp.text
    except: pass
    # Fallback: gviz/tq
    m = re.search(r"spreadsheets/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m:
        sid = m.group(1); gm = re.search(r"[?#&]gid=([0-9]+)", url, re.I)
        gid = gm.group(1) if gm else "0"
        gviz = f"https://docs.google.com/spreadsheets/d/{sid}/gviz/tq?tqx=out:csv&gid={gid}"
        resp2 = requests.get(gviz, timeout=30, headers=headers)
        if resp2.status_code == 200:
            return resp2.text
    raise Exception("Não foi possível carregar a planilha. Verifique se está compartilhada como 'Qualquer pessoa com o link pode ver'.")

def extract_gdrive_id(url):
    if not url: return None
    m = re.search(r"drive\.google\.com/.*/d/([A-Za-z0-9\-_]+)", url, re.I)
    if m: return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9\-_]+)", url, re.I)
    if m: return m.group(1)
    if "/" not in url and re.match(r"^[A-Za-z0-9\-_]+$", url): return url
    return None

# ─── Default Schedule ─────────────────────────────────────────────────────────
DEF_SAB = [
    ("07:00","07:30","Chegada dos Encontristas","",5),("07:30","08:30","CAFÉ","",3),
    ("08:30","09:40","PENIEL (C/Ministração)","Fabiana Granja",0),
    ("09:45","10:50","O PECADO E SUAS CONSEQUÊNCIAS (C/Ministração)","Anderson Diocesano",0),
    ("10:55","11:10","Coffee Break","",2),("11:15","12:25","LIBERTAÇÃO (C/Ministração)","Pr. João Paulo",0),
    ("12:30","13:40","CURA INTERIOR (C/Ministração)","Ednalva Araújo",0),
    ("13:40","15:00","ALMOÇO e DESCANSO","",3),
    ("15:00","16:30","NOVA VIDA EM CRISTO (C/Ministração)","Leandro Santos",0),
    ("16:35","16:50","Coffee Break","",2),
    ("16:55","18:00","A VISÃO DOS GC (A IMPORTÂNCIA) (C/Ministração)","Pr. Leandro Miranda",0),
    ("18:00","19:10","BANHO","",5),("19:20","20:25","JANTAR","",3),
    ("20:30","21:55","REDENÇÃO FINANCEIRA (C/Ministração)","Pr. Ailton Siqueira",0),
    ("22:00","23:20","A CRUZ (C/Ministração)","Wallace Mendes",0),
    ("23:25","23:59","Coffee Break / Dormir","",2)]
DEF_DOM = [
    ("07:00","07:30","DESPERTAR DOS ENCONTRISTAS","",5),("07:30","08:30","CAFÉ","",3),
    ("08:30","10:00","SONHOS (C/Ministração)","Jonas Assunção",0),
    ("10:05","10:20","Coffee Break","",2),("10:25","11:50","LOUVOR / ADORAÇÃO","",1),
    ("11:55","13:00","O ESPÍRITO SANTO (C/Ministração)","Pr. João Paulo",0),
    ("13:00","14:30","ALMOÇO e DESCANSO","",3),
    ("14:30","16:00","BATISMO / ALIANÇA","Pr. João Paulo",0),
    ("16:00","16:30","Coffee Break","",2),("16:30","17:30","Entrega das Cartas","",5),
    ("17:30","18:00","Encerramento","",5)]

def seed_schedule(eid):
    if load_schedule(eid): return
    sb = get_sb()
    for i,(s,e,t,sp,k) in enumerate(DEF_SAB):
        sb.table("ScheduleItems").insert({"Id":str(uuid.uuid4()),"EventId":eid,"Day":"Sábado","Start":s,"End":e,"Title":t,"Speaker":sp or None,"Kind":k,"Order":i,"CreatedAtUtc":utcnow()}).execute()
    for i,(s,e,t,sp,k) in enumerate(DEF_DOM):
        sb.table("ScheduleItems").insert({"Id":str(uuid.uuid4()),"EventId":eid,"Day":"Domingo","Start":s,"End":e,"Title":t,"Speaker":sp or None,"Kind":k,"Order":i,"CreatedAtUtc":utcnow()}).execute()

# ═══════════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════════════
def show_login():
    st.markdown("## 🔐 Gestão Encontro com Deus"); st.caption("Informe a senha para acessar.")
    pwd = st.text_input("Senha", type="password", key="lp")
    if st.button("Entrar", type="primary", use_container_width=True):
        if pwd == st.secrets.get("APP_PASSWORD", "encontro2025"): st.session_state.authenticated = True; st.rerun()
        else: st.error("Senha incorreta.")

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
                if st.button("Abrir →", key=f"d_{ev['Id']}", use_container_width=True): st.session_state.current_event = ev["Id"]; st.session_state.page = "dashboard"; st.rerun()
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
        with c2: end = st.date_input("Data Fim")
        loc = st.text_input("Local (opcional)", placeholder="Ex.: Sítio Betel")
        if st.form_submit_button("Salvar e abrir Dashboard", type="primary"):
            if not name or len(name) < 3: st.error("Nome mín. 3 caracteres."); return
            nid = str(uuid.uuid4())
            get_sb().table("Events").insert({"Id": nid, "Name": name, "StartDate": start.isoformat(), "EndDate": end.isoformat(), "Location": loc or None, "Status": "Config", "CreatedAtUtc": utcnow()}).execute()
            seed_schedule(nid); st.session_state.current_event = nid; st.session_state.page = "dashboard"; st.rerun()

def event_sidebar(eid):
    ev = load_event(eid)
    if not ev: st.error("Evento não encontrado."); st.session_state.page = "events"; st.rerun(); return None
    with st.sidebar:
        st.markdown(f"### {ev['Name']}")
        if ev.get("StartDate"): st.caption(f"📅 {fmt_date_br(ev['StartDate'])} — {fmt_date_br(ev.get('EndDate'))}")
        if ev.get("Location"): st.caption(f"📍 {ev['Location']}")
        st.divider()
        for k, label in {"dashboard":"📊 Dashboard","participants":"👥 Participantes","rooms":"🏠 Quartos","schedule":"📋 Cronograma","letters":"💌 Cartas","labels":"🏷️ Etiquetas","photos":"📸 Fotos","secretary":"🗂️ Secretária","settings":"⚙️ Config"}.items():
            if st.button(label, key=f"n_{k}", use_container_width=True, type="primary" if st.session_state.get("page")==k else "secondary"): st.session_state.page = k; st.rerun()
        st.divider()
        if st.button("← Todos os eventos", use_container_width=True): st.session_state.pop("current_event", None); st.session_state.page = "events"; st.rerun()
    return ev

def page_dashboard(eid, ev):
    st.markdown(f"## 📊 Dashboard — {ev['Name']}")
    parts = load_participants(eid)
    enc = [p for p in parts if is_encounterist(p.get("Category"))]
    srv_com = [p for p in parts if is_server_with_shirt(p.get("Category"))]
    srv_sem = [p for p in parts if is_server_no_shirt(p.get("Category"))]
    equipe = [p for p in parts if norm(p.get("Category","")).startswith("equipe")]
    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Encontristas (M)", sum(1 for p in enc if p.get("Gender")==1))
    c2.metric("Encontristas (F)", sum(1 for p in enc if p.get("Gender")==2))
    c3.metric("Servos c/ camisa", len(srv_com)); c4.metric("Servos s/ camisa", len(srv_sem))
    c5,c6,c7,c8 = st.columns(4)
    c5.metric("Equipe", len(equipe)); c6.metric("Total Geral", len(parts)); c7.metric("Quartos", len(load_rooms(eid))); c8.metric("","")
    def sb(title, items):
        st.markdown(f"#### {title}"); counts = {}
        for p in items:
            sz = SHIRT_MAP.get(p.get("ShirtSize",0),"-")
            if sz != "-": counts[sz] = counts.get(sz,0)+1
        cols = st.columns(len(SHIRT_KEYS))
        for i, k in enumerate(SHIRT_KEYS): cols[i].metric(k, counts.get(k,0))
    sb("Camisetas — Encontristas", enc); sb("Camisetas — Servos", srv_com); sb("Camisetas — Total", enc+srv_com)

def page_participants(eid, ev):
    st.markdown(f"## 👥 Participantes — {ev['Name']}")
    with st.expander("📥 Importar CSV", expanded=False):
        uploaded = st.file_uploader("Arquivo CSV", type=["csv"], key="cu")
        replace = st.checkbox("Substituir existentes", value=False)
        if st.button("Importar", type="primary") and uploaded:
            with st.spinner("Importando..."):
                try: ok, fail = import_csv(eid, uploaded.read(), replace); st.success(f"✅ {ok} importados, {fail} falha(s)."); st.rerun()
                except Exception as e: st.error(f"Erro: {e}")
    with st.expander("➕ Adicionar manualmente", expanded=False):
        with st.form("ap"):
            a1,a2 = st.columns(2)
            with a1: pn=st.text_input("Nome*"); pg=st.selectbox("Gênero",["Masculino","Feminino","Não informado"]); pp=st.text_input("Telefone"); pb=st.text_input("Nascimento (dd/mm/aaaa)")
            with a2: pc=st.selectbox("Categoria",CATEGORY_OPTIONS); ps=st.selectbox("Camiseta",["-"]+SHIRT_KEYS); psc=st.text_input("Setor"); pgc=st.text_input("Grupo de Conexão")
            if st.form_submit_button("Adicionar"):
                if not pn: st.error("Nome obrigatório."); return
                get_sb().table("Participants").insert({"Id":str(uuid.uuid4()),"EventId":eid,"Name":pn,"Gender":GENDER_REV.get(pg,0),"ShirtSize":SHIRT_REV.get(ps,0),"Category":pc,"Phone":pp or None,"BirthDate":parse_date_br(pb) if pb else None,"ConnectionSector":psc or None,"ConnectionGroup":pgc or None,"CreatedAtUtc":utcnow()}).execute()
                st.success(f"'{pn}' adicionado!"); st.rerun()

    parts = load_participants(eid); assigns = load_assignments(eid); rooms = load_rooms(eid)
    rm = {r["Id"]: r["Name"] for r in rooms}; pr = {a["ParticipantId"]: rm.get(a["RoomId"],"-") for a in assigns}

    # ── Filtros ──────────────────────────────────────────────────────────────
    fc1,fc2,fc3,fc4 = st.columns(4)
    with fc1: search = st.text_input("🔍 Nome", key="ps")
    with fc2: fg = st.selectbox("Sexo", ["Todos","Masculino","Feminino"], key="fg")
    with fc3: fc_sel = st.selectbox("Categoria", ["Todas","Encontrista","Servo","Equipe"], key="fc")
    with fc4: fsc = st.text_input("Setor", key="fsc")
    filtered = parts
    if search: filtered = [p for p in filtered if search.lower() in p["Name"].lower()]
    if fg=="Masculino": filtered = [p for p in filtered if p.get("Gender")==1]
    elif fg=="Feminino": filtered = [p for p in filtered if p.get("Gender")==2]
    if fc_sel=="Encontrista": filtered = [p for p in filtered if is_encounterist(p.get("Category"))]
    elif fc_sel=="Servo": filtered = [p for p in filtered if is_server(p.get("Category"))]
    elif fc_sel=="Equipe": filtered = [p for p in filtered if norm(p.get("Category","")).startswith("equipe")]
    if fsc: filtered = [p for p in filtered if fsc.lower() in norm(p.get("ConnectionSector",""))]

    st.markdown(f"**{len(filtered)} participante(s)**")

    # ── Botões de ação — NO TOPO ──────────────────────────────────────────────
    ac1, ac2, ac3 = st.columns(3)
    with ac1:
        if st.button("📄 PDF por Setor", use_container_width=True, type="primary"):
            if filtered:
                with st.spinner("Gerando PDF..."):
                    pdf = generate_sector_pdf(filtered, ev_name=ev["Name"], assigns=assigns, rooms=rooms)
                    st.download_button("⬇️ Baixar PDF", pdf, "participantes_por_setor.pdf", "application/pdf", use_container_width=True)
            else:
                st.warning("Nenhum participante para exportar.")
    with ac2:
        if st.button("📄 Exportar CSV", use_container_width=True):
            if filtered:
                rows_exp = [{
                    "Nome": p["Name"],
                    "Categoria": p.get("Category") or "-",
                    "Sexo": GENDER_MAP.get(p.get("Gender",0),"-"),
                    "Camisa": SHIRT_MAP.get(p.get("ShirtSize",0),"-"),
                    "Quarto": pr.get(p["Id"],"-"),
                    "Telefone": p.get("Phone") or "-",
                    "Setor": p.get("ConnectionSector") or "-",
                    "GC": p.get("ConnectionGroup") or "-",
                    "Convidado por": p.get("InvitedBy") or "-",
                } for p in filtered]
                st.download_button("⬇️ Baixar CSV", pd.DataFrame(rows_exp).to_csv(index=False), "participantes.csv", "text/csv", use_container_width=True)
    with ac3:
        if st.button("🗑️ Limpar TODOS", use_container_width=True):
            sb2 = get_sb(); sb2.table("RoomAssignments").delete().eq("EventId", eid).execute(); sb2.table("Participants").delete().eq("EventId", eid).execute(); st.success("Removidos."); st.rerun()

    st.divider()

    # ── Lista ────────────────────────────────────────────────────────────────
    if not filtered:
        st.info("Nenhum participante com os filtros aplicados.")
    else:
        for p in filtered:
            pid = p["Id"]
            edit_key = f"pedit_{pid}"
            age = age_from(p.get("BirthDate"))
            quarto = pr.get(pid, "-")
            with st.container(border=True):
                c1, c2, c3 = st.columns([6, 1, 1])
                with c1:
                    st.markdown(f"**{p['Name']}** · {GENDER_MAP.get(p.get('Gender',0),'-')} · {age or '?'} anos · Camisa: {SHIRT_MAP.get(p.get('ShirtSize',0),'-')}")
                    invited_str = f" · Convidado por: {p['InvitedBy']}" if p.get("InvitedBy") else ""
                    st.caption(f"{p.get('Category') or '-'} · Quarto: {quarto} · Setor: {p.get('ConnectionSector') or '-'} · GC: {p.get('ConnectionGroup') or '-'}{invited_str}")
                with c2:
                    if st.button("✏️", key=f"ebtn_{pid}", use_container_width=True):
                        st.session_state[edit_key] = not st.session_state.get(edit_key, False); st.rerun()
                with c3:
                    if st.button("🗑️", key=f"delbtn_{pid}", use_container_width=True):
                        sb2 = get_sb(); sb2.table("RoomAssignments").delete().eq("ParticipantId", pid).execute(); sb2.table("Participants").delete().eq("Id", pid).execute(); st.rerun()

                if st.session_state.get(edit_key, False):
                    with st.form(f"pform_{pid}"):
                        ea, eb = st.columns(2)
                        with ea:
                            en = st.text_input("Nome", value=p.get("Name",""))
                            eg = st.selectbox("Gênero", ["Masculino","Feminino","Não informado"], index=max(0, p.get("Gender",0)-1) if p.get("Gender",0)>0 else 2)
                            ecat = st.selectbox("Categoria", CATEGORY_OPTIONS, index=CATEGORY_OPTIONS.index(p["Category"]) if p.get("Category") in CATEGORY_OPTIONS else 0)
                            ephone = st.text_input("Telefone", value=p.get("Phone") or "")
                        with eb:
                            shirt_opts = ["-"] + SHIRT_KEYS
                            cur_shirt = SHIRT_MAP.get(p.get("ShirtSize",0),"-")
                            esh = st.selectbox("Camisa", shirt_opts, index=shirt_opts.index(cur_shirt) if cur_shirt in shirt_opts else 0)
                            esc = st.text_input("Setor", value=p.get("ConnectionSector") or "")
                            egc = st.text_input("GC", value=p.get("ConnectionGroup") or "")
                            einv = st.text_input("Indicado por", value=p.get("InvitedBy") or "")
                        if st.form_submit_button("💾 Salvar", type="primary"):
                            get_sb().table("Participants").update({
                                "Name": en, "Gender": GENDER_REV.get(eg, 0),
                                "ShirtSize": SHIRT_REV.get(esh, 0), "Category": ecat,
                                "Phone": ephone or None, "ConnectionSector": esc or None,
                                "ConnectionGroup": egc or None, "InvitedBy": einv or None,
                                "UpdatedAtUtc": utcnow()}).eq("Id", pid).execute()
                            st.session_state.pop(edit_key, None); st.rerun()

def page_rooms(eid, ev):
    st.markdown(f"## 🏠 Quartos — {ev['Name']}")
    parts = load_participants(eid); rooms = load_rooms(eid); assigns = load_assignments(eid)
    pm = {p["Id"]: p for p in parts}; sb2 = get_sb()

    # ── Contador sem quarto em cima ──────────────────────────────────────────
    aids_top = set(a["ParticipantId"] for a in assigns)
    unall_top = [p for p in parts if p["Id"] not in aids_top and is_encounterist(p.get("Category"))]
    if unall_top:
        unall_m = sum(1 for p in unall_top if p.get("Gender")==1)
        unall_f = sum(1 for p in unall_top if p.get("Gender")==2)
        st.warning(f"⚠️ **{len(unall_top)} encontrista(s) sem quarto atribuído** — {unall_m} Masculino · {unall_f} Feminino")

    with st.expander("➕ Novo Quarto", expanded=False):
        with st.form("nr"):
            rc1,rc2,rc3 = st.columns(3)
            with rc1: rn = st.text_input("Nome do Quarto")
            with rc2: rcap = st.number_input("Capacidade", min_value=1, value=10)
            with rc3: rg = st.selectbox("Gênero", ["Feminino","Masculino"], key="rg")
            if st.form_submit_button("Criar"):
                if not rn: st.error("Nome obrigatório."); return
                sb2.table("Rooms").insert({"Id":str(uuid.uuid4()),"EventId":eid,"Name":rn,"Capacity":rcap,"Gender":2 if rg=="Feminino" else 1,"CreatedAtUtc":utcnow()}).execute(); st.rerun()
    b1,b2,b3 = st.columns(3)
    with b1:
        if st.button("🔀 Distribuir automaticamente", type="primary"):
            n, nr, err = distribute_rooms(eid)
            if err: st.error(err)
            else: st.success(f"✅ {n} alocados em {nr} quarto(s)."); st.rerun()
    with b2:
        if st.button("📄 PDF quartos"):
            pdf = generate_rooms_pdf(eid); st.download_button("⬇️ Baixar", pdf, "quartos.pdf", "application/pdf")
    if not rooms: st.info("Nenhum quarto criado."); return
    am = {}; aids = set()
    for a in assigns: am.setdefault(a["RoomId"], []).append(a); aids.add(a["ParticipantId"])
    for room in rooms:
        rid = room["Id"]; occs = am.get(rid, []); leader = pm.get(room.get("LeaderId")) if room.get("LeaderId") else None
        with st.container(border=True):
            h1,h2,h3,h4 = st.columns([3,2,1,1])
            with h1: st.markdown(f"**{room['Name']}** ({GENDER_MAP.get(room.get('Gender',0),'-')})"); st.caption(f"{len(occs)}/{room['Capacity']} · Líder: {leader['Name'] if leader else '-'}")
            with h2:
                servos = [p for p in parts if is_server(p.get("Category"))]
                opts = ["(nenhum)"] + [p["Name"] for p in servos]
                ci = opts.index(leader["Name"]) if leader and leader["Name"] in opts else 0
                sel = st.selectbox("Líder", opts, index=ci, key=f"l_{rid}")
                if st.button("Salvar", key=f"sl_{rid}"):
                    nlid = next((p["Id"] for p in servos if p["Name"]==sel), None) if sel!="(nenhum)" else None
                    sb2.table("Rooms").update({"LeaderId":nlid,"UpdatedAtUtc":utcnow()}).eq("Id",rid).execute(); st.rerun()
            with h3:
                exp_key = f"exp_{rid}"
                if exp_key not in st.session_state: st.session_state[exp_key] = False
                lbl = "🔼 Recolher" if st.session_state[exp_key] else "🔽 Expandir"
                if st.button(lbl, key=f"eb_{rid}"): st.session_state[exp_key] = not st.session_state[exp_key]; st.rerun()
            with h4:
                if st.button("🗑️", key=f"dr_{rid}"): sb2.table("RoomAssignments").delete().eq("RoomId",rid).execute(); sb2.table("Rooms").delete().eq("Id",rid).execute(); st.rerun()

            if st.session_state.get(f"exp_{rid}", False):
                if occs:
                    for a in occs:
                        p = pm.get(a["ParticipantId"])
                        if not p: continue
                        age = age_from(p.get("BirthDate"))
                        other_rooms = [r for r in rooms if r["Id"] != rid and (r.get("Gender",0)==p.get("Gender",0) or r.get("Gender",0)==0)]
                        sw_key = f"sw_{a['Id']}"
                        o1, o2, o3, o4, o5 = st.columns([4, 2, 3, 1, 1])
                        with o1: st.text(f"  • {p['Name']} ({age or '?'}) — {SHIRT_MAP.get(p.get('ShirtSize',0),'-')}")
                        with o2: st.text(p.get("ConnectionGroup") or "-")
                        with o3:
                            if other_rooms:
                                st.selectbox("↔ Quarto", ["—"] + [r["Name"] for r in other_rooms], key=sw_key, label_visibility="collapsed")
                        with o4:
                            if other_rooms:
                                if st.button("↔", key=f"swb_{a['Id']}", help="Confirmar troca"):
                                    chosen = st.session_state.get(sw_key, "—")
                                    if chosen and chosen != "—":
                                        new_rid = next((r["Id"] for r in other_rooms if r["Name"]==chosen), None)
                                        if new_rid:
                                            sb2.table("RoomAssignments").update({"RoomId":new_rid,"UpdatedAtUtc":utcnow()}).eq("Id",a["Id"]).execute(); st.rerun()
                        with o5:
                            if st.button("✕", key=f"u_{rid}_{a['Id']}"): sb2.table("RoomAssignments").delete().eq("Id",a["Id"]).execute(); st.rerun()
                unass = [p for p in parts if p["Id"] not in aids and is_encounterist(p.get("Category")) and (p.get("Gender",0)==room.get("Gender",0) or room.get("Gender",0)==0)]
                if unass and len(occs) < room["Capacity"]:
                    sp = st.selectbox("Adicionar encontrista", [""]+[p["Name"] for p in unass], key=f"a_{rid}")
                    if sp and st.button("＋", key=f"ba_{rid}"):
                        pid = next(p["Id"] for p in unass if p["Name"]==sp)
                        sb2.table("RoomAssignments").insert({"Id":str(uuid.uuid4()),"EventId":eid,"RoomId":rid,"ParticipantId":pid,"CreatedAtUtc":utcnow()}).execute(); st.rerun()

# ─── Schedule helpers ─────────────────────────────────────────────────────────
def schedule_sort_key(item):
    """Ordena por horário de início (HH:MM). Itens sem horário vão pro fim."""
    t = time_to_minutes(item.get("Start", ""))
    return t if t is not None else 9999

def check_schedule_conflict(items, day, new_start, new_end, exclude_id=None):
    """
    Retorna lista de itens que colidem com o intervalo new_start–new_end no mesmo dia.
    Colisão: os intervalos se sobrepõem (não apenas tocam).
    """
    ns = time_to_minutes(new_start)
    ne = time_to_minutes(new_end)
    conflicts = []
    for it in items:
        if it.get("Day") != day: continue
        if exclude_id and it.get("Id") == exclude_id: continue
        es = time_to_minutes(it.get("Start"))
        ee = time_to_minutes(it.get("End"))
        if es is None or ee is None or ns is None: continue
        # Sobreposição: um começa antes do outro terminar
        if ns < ee and (ne is None or ne > es):
            conflicts.append(it)
    return conflicts

def generate_schedule_pdf(items, ev_name):
    from reportlab.lib.pagesizes import A4; from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=36, bottomMargin=36)
    styles = getSampleStyleSheet(); elems = []
    elems.append(Paragraph(f"Cronograma — {ev_name}", styles["Title"]))
    elems.append(Spacer(1,12))
    days = {}
    for it in items: days.setdefault(it.get("Day","?"), []).append(it)
    day_order = ["Sábado","Domingo"]
    for dn in sorted(days.keys(), key=lambda d: day_order.index(d) if d in day_order else 99):
        di = days[dn]
        elems.append(Paragraph(f"📅 {dn}", styles["Heading2"])); elems.append(Spacer(1,6))
        data = [["Horário","Tipo","Título","Palestrante"]]
        for it in sorted(di, key=schedule_sort_key):
            kl = KIND_MAP.get(it.get("Kind",0),"?")
            data.append([f"{it.get('Start','')}–{it.get('End','')}", kl, it.get("Title",""), it.get("Speaker") or ""])
        t = Table(data, colWidths=[70, 60, 250, 120])
        t.setStyle(TableStyle([("BACKGROUND",(0,0),(-1,0),colors.HexColor("#F1F5F9")),("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
            ("FONTSIZE",(0,0),(-1,-1),9),("GRID",(0,0),(-1,-1),0.5,colors.HexColor("#CBD5E1")),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4)]))
        elems.append(t); elems.append(Spacer(1,12))
    elems.append(Spacer(1,20))
    elems.append(Paragraph(f"Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')}", styles["Normal"]))
    doc.build(elems); return buf.getvalue()

def page_schedule(eid, ev):
    st.markdown(f"## 📋 Cronograma — {ev['Name']}")
    seed_schedule(eid)
    items = load_schedule(eid)

    # ── Formulário Novo Item ──────────────────────────────────────────────────
    with st.expander("➕ Novo Item", expanded=False):
        ns_dia = st.selectbox("Dia", ["Sábado","Domingo"], key="ns_dia")
        ns_titulo = st.text_input("Título*", key="ns_titulo")
        nc1, nc2 = st.columns(2)
        with nc1:
            ns_inicio = st.text_input("Início (HH:MM)", key="ns_inicio")
            ns_fim = st.text_input("Fim (HH:MM)", key="ns_fim")
        with nc2:
            ns_palestrante = st.text_input("Palestrante", key="ns_palestrante")
            ns_tipo = st.selectbox("Tipo", list(KIND_REV.keys()), key="ns_tipo")

        if st.button("➕ Adicionar item", type="primary", key="ns_add"):
            if not ns_titulo:
                st.error("Título obrigatório.")
            else:
                # ── Verificação de conflito de horário ──────────────────────
                conflicts = check_schedule_conflict(items, ns_dia, ns_inicio, ns_fim)
                if conflicts:
                    conflict_list = "; ".join(f"**{c.get('Start','')}–{c.get('End','')} {c.get('Title','')}**" for c in conflicts)
                    st.warning(f"⚠️ Conflito de horário! Já existe item(ns) nesse intervalo: {conflict_list}")
                    st.caption("Corrija o horário ou confirme mesmo assim clicando abaixo.")
                    if st.button("⚡ Adicionar mesmo assim", key="ns_force"):
                        eo = [i["Order"] for i in items if i.get("Day")==ns_dia]
                        no = max(eo, default=-1) + 1
                        get_sb().table("ScheduleItems").insert({
                            "Id": str(uuid.uuid4()), "EventId": eid, "Day": ns_dia,
                            "Start": ns_inicio, "End": ns_fim, "Title": ns_titulo,
                            "Speaker": ns_palestrante or None, "Kind": KIND_REV[ns_tipo],
                            "Order": no, "CreatedAtUtc": utcnow()
                        }).execute()
                        st.success(f"'{ns_titulo}' adicionado com conflito registrado."); st.rerun()
                else:
                    eo = [i["Order"] for i in items if i.get("Day")==ns_dia]
                    no = max(eo, default=-1) + 1
                    get_sb().table("ScheduleItems").insert({
                        "Id": str(uuid.uuid4()), "EventId": eid, "Day": ns_dia,
                        "Start": ns_inicio, "End": ns_fim, "Title": ns_titulo,
                        "Speaker": ns_palestrante or None, "Kind": KIND_REV[ns_tipo],
                        "Order": no, "CreatedAtUtc": utcnow()
                    }).execute()
                    st.success(f"'{ns_titulo}' adicionado!"); st.rerun()

    # Export PDF
    if items:
        if st.button("📄 Exportar Cronograma PDF"):
            pdf = generate_schedule_pdf(items, ev["Name"])
            st.download_button("⬇️ Baixar PDF", pdf, "cronograma.pdf", "application/pdf")

    if not items: st.info("Cronograma vazio."); return

    days = {}
    for it in items: days.setdefault(it.get("Day","?"), []).append(it)
    day_order = ["Sábado","Domingo"]
    for dn in sorted(days.keys(), key=lambda d: day_order.index(d) if d in day_order else 99):
        di = days[dn]
        st.markdown(f"### 📅 {dn}")
        # ── Ordena por horário de início ──────────────────────────────────
        for it in sorted(di, key=schedule_sort_key):
            kl = KIND_MAP.get(it.get("Kind",0),"?")
            em = {"Palestra":"🎤","Louvor":"🎶","Intervalo":"☕","Refeição":"🍽️","Dinâmica":"🎯","Outro":"📌"}.get(kl,"")
            edit_key = f"edit_{it['Id']}"
            with st.container(border=True):
                i1,i2,i3,i4 = st.columns([1,4,1,1])
                with i1:
                    st.markdown(f"**{it.get('Start','')}–{it.get('End','')}**")
                with i2:
                    st.markdown(f"{em} **{it['Title']}**")
                    if it.get("Speaker"):
                        st.caption(f"🎤 {it['Speaker']}")
                with i3:
                    if st.button("✏️", key=f"ed_{it['Id']}"):
                        st.session_state[edit_key] = not st.session_state.get(edit_key, False)
                        st.rerun()
                with i4:
                    if st.button("🗑️", key=f"ds_{it['Id']}"):
                        get_sb().table("ScheduleItems").delete().eq("Id",it["Id"]).execute(); st.rerun()

                if st.session_state.get(edit_key, False):
                    with st.form(f"ef_{it['Id']}"):
                        ec1,ec2,ec3 = st.columns(3)
                        with ec1:
                            new_title = st.text_input("Título", value=it.get("Title",""))
                            new_speaker = st.text_input("Palestrante", value=it.get("Speaker") or "")
                        with ec2:
                            new_start = st.text_input("Início", value=it.get("Start",""))
                            new_end = st.text_input("Fim", value=it.get("End",""))
                        with ec3:
                            kind_opts = list(KIND_REV.keys())
                            cur_kind = KIND_MAP.get(it.get("Kind",0),"Outro")
                            new_kind = st.selectbox("Tipo", kind_opts, index=kind_opts.index(cur_kind) if cur_kind in kind_opts else 0)
                            new_day = st.selectbox("Dia", ["Sábado","Domingo"], index=0 if it.get("Day")=="Sábado" else 1)
                        if st.form_submit_button("💾 Salvar alterações"):
                            # Verifica conflito também no edit (excluindo o próprio item)
                            edit_conflicts = check_schedule_conflict(items, new_day, new_start, new_end, exclude_id=it["Id"])
                            if edit_conflicts:
                                conflict_list = "; ".join(f"{c.get('Start','')}–{c.get('End','')} {c.get('Title','')}" for c in edit_conflicts)
                                st.warning(f"⚠️ Conflito de horário com: {conflict_list} — salvo mesmo assim.")
                            get_sb().table("ScheduleItems").update({
                                "Title":new_title,"Speaker":new_speaker or None,
                                "Start":new_start,"End":new_end,
                                "Kind":KIND_REV[new_kind],"Day":new_day,
                                "UpdatedAtUtc":utcnow()
                            }).eq("Id",it["Id"]).execute()
                            st.session_state.pop(edit_key, None); st.rerun()

def generate_letters_docx(participant_name, letters_list):
    """Gera um .docx com uma carta por página para o encontrista."""
    from docx import Document
    from docx.shared import Pt, Inches
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    doc = Document()
    for section in doc.sections:
        section.top_margin = Inches(1); section.bottom_margin = Inches(1)
        section.left_margin = Inches(1.2); section.right_margin = Inches(1.2)
    for i, carta in enumerate(letters_list):
        if i > 0:
            doc.add_page_break()
        h = doc.add_paragraph()
        h.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run_h = h.add_run(f"Para: {participant_name}")
        run_h.bold = True; run_h.font.size = Pt(14)
        doc.add_paragraph()
        sender_p = doc.add_paragraph()
        run_s = sender_p.add_run(f"De: {carta.get('sender','—')}")
        run_s.bold = True; run_s.font.size = Pt(11)
        doc.add_paragraph()
        msg_p = doc.add_paragraph(carta.get("message",""))
        if msg_p.runs: msg_p.runs[0].font.size = Pt(11)
        doc.add_paragraph()
        footer_p = doc.add_paragraph(f"— Carta {i+1} de {len(letters_list)} —")
        footer_p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        if footer_p.runs: footer_p.runs[0].font.size = Pt(9)
    buf = io.BytesIO(); doc.save(buf); buf.seek(0); return buf.getvalue()

def page_letters(eid, ev):
    st.markdown(f"## 💌 Cartas — {ev['Name']}")
    url = ev.get("LettersSheetUrl") or ""
    parts = load_participants(eid); enc = [p for p in parts if is_encounterist(p.get("Category"))]

    if not url:
        st.warning("⚠️ URL da planilha de cartas não configurada. Acesse **Configurações** para adicionar.")
        return

    dl_key = f"letters_dl_{eid}"
    prev_counts_key = f"letters_prev_counts_{eid}"

    dl_info = st.session_state.get(dl_key)
    if dl_info:
        st.caption(f"📥 Último carregamento: **{dl_info['ts']}** · {dl_info['total']} carta(s)")

    if st.button("🔍 Carregar cartas", type="primary"):
        with st.spinner("Lendo..."):
            try:
                text = fetch_sheet_csv(url)
                df = pd.read_csv(io.StringIO(text), dtype=str); df.columns = [c.strip() for c in df.columns]
                ct = find_header(df.columns, ["Para quem","Destinatário","Para","Nome do Encontrista","Encontrista","Nome do encontrista:","Nome do encontrista"])
                cf = find_header(df.columns, ["De quem","Remetente","De","Nome de quem escreve","Seu nome","Seu nome:"])
                cm = find_header(df.columns, ["Mensagem","Carta","Texto","Conteúdo","Escreva algo especial para ele(a) aqui:","Escreva algo especial para ele(a) aqui","Mensagem para ele(a)"])
                if not ct: st.error(f"Coluna 'Para quem' não encontrada. Colunas: {', '.join(df.columns)}"); return
                if not cm: st.error(f"Coluna 'Mensagem' não encontrada. Colunas: {', '.join(df.columns)}"); return
                letters = {}
                for _, row in df.iterrows():
                    to = safe_str(row.get(ct)); sender = safe_str(row.get(cf)) if cf else "—"
                    if sender is None: sender = "—"
                    msg = safe_str(row.get(cm)) or ""
                    if to and msg: letters.setdefault(to, []).append({"sender": sender, "message": msg})
                total = sum(len(v) for v in letters.values())
                old_letters = st.session_state.get(f"letters_data_{eid}", {})
                old_counts = {}
                for p2 in enc:
                    c2 = sum(len(lts) for k2, lts in old_letters.items() if norm(k2)==norm(p2["Name"]) or p2["Name"].lower() in k2.lower() or k2.lower() in p2["Name"].lower())
                    old_counts[p2["Id"]] = c2
                st.session_state[prev_counts_key] = old_counts
                st.session_state[dl_key] = {"ts": datetime.now().strftime("%d/%m/%Y às %H:%M"), "total": total}
                st.session_state[f"letters_data_{eid}"] = letters
                st.success(f"✅ {total} carta(s) carregadas")
            except Exception as e:
                st.error(f"Erro ao carregar planilha: {e}")
                st.caption("💡 Verifique se a planilha está compartilhada como 'Qualquer pessoa com o link pode ver'.")
                return

    letters = st.session_state.get(f"letters_data_{eid}", {})
    prev_counts = st.session_state.get(prev_counts_key, {})
    sec_status = st.session_state.get(f"sec_status_{eid}", {})

    st.divider()
    sem_carta = sum(1 for p in enc if sum(len(lts) for key, lts in letters.items() if norm(key)==norm(p["Name"]) or p["Name"].lower() in key.lower() or key.lower() in p["Name"].lower()) == 0)
    st.markdown(f"**{len(enc)} encontrista(s)** · {sem_carta} sem carta recebida")

    search_l = st.text_input("🔍 Filtrar por nome", key="ltr_search")
    enc_filtrado = [p for p in enc if search_l.lower() in p["Name"].lower()] if search_l else enc

    for p in enc_filtrado:
        pn = p["Name"]; ml = []
        for key, lts in letters.items():
            if norm(key)==norm(pn) or pn.lower() in key.lower() or key.lower() in pn.lower():
                ml.extend(lts)
        count = len(ml)

        prev_p = prev_counts.get(p["Id"], count)
        novas_p = max(0, count - prev_p)
        novas_str = f" · **+{novas_p} nova(s)**" if novas_p > 0 else ""

        finalizado = sec_status.get(p["Id"], {}).get("bolsa_ok", False)
        if finalizado: icon = "✅"; label_extra = " — FINALIZADO ✅"
        else: icon = "🚨" if count==0 else ("⚠️" if count<=3 else "✅"); label_extra = ""

        col_info, col_btn = st.columns([6, 2])
        with col_info:
            st.markdown(f"{icon} **{pn}** — {count} carta(s){novas_str}{label_extra}")
        with col_btn:
            if count > 0:
                try:
                    docx_bytes = generate_letters_docx(pn, ml)
                    nome_arquivo = "".join(c for c in pn if c.isalnum() or c in " _-").strip().replace(" ","_")
                    st.download_button("⬇️ Download", docx_bytes, f"cartas_{nome_arquivo}.docx",
                        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        key=f"dl_docx_{p['Id']}", use_container_width=True)
                except Exception as e:
                    st.error(f"Erro: {e}")
            else:
                st.caption("Sem cartas" if letters else "—")

def generate_labels_pimaco(parts_sel, label_type, assigns, rooms):
    """
    Gera PDF no formato Pimaco 6182.
    Especificações reais: 33,9 x 101,6 mm · 2 colunas × 7 linhas = 14 etiquetas/folha.
    Papel Carta (215,9 × 279,4 mm).
    Margens: lateral 6,35 mm · topo 21,05 mm · gap entre colunas = 0 (etiquetas coladas).
    """
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas as rl_canvas
    from reportlab.lib import colors as rl_colors
    import math as _math

    MM = 2.8346  # 1 mm em pontos

    rm = {r["Id"]: r["Name"] for r in rooms}
    pr = {a["ParticipantId"]: rm.get(a["RoomId"], "-") for a in assigns}

    # Papel carta em pontos
    PAGE_W, PAGE_H = letter  # 612 × 792 pt

    # Dimensões da etiqueta em pontos
    LBL_W = 101.6 * MM   # 288.0 pt
    LBL_H = 33.9  * MM   #  96.1 pt

    # Layout: 2 colunas × 7 linhas = 14 por folha
    COLS = 2
    ROWS = 7

    # Margens Pimaco 6182 (calculadas das medidas físicas)
    MARGIN_LEFT = 6.35 * MM   # 18.0 pt
    MARGIN_TOP  = PAGE_H - (21.05 * MM)  # topo da primeira linha (y do topo da folha menos margem)

    PER_PAGE = COLS * ROWS  # 14

    def draw_name_wrapped(c, text, x, y, max_w, font_bold, font_size_big, font_size_small):
        """Desenha nome — quebra em 2 linhas se necessário, ou reduz fonte."""
        font = font_bold
        # Tenta tamanho original
        c.setFont(font, font_size_big)
        if c.stringWidth(text, font, font_size_big) <= max_w:
            c.drawString(x, y, text)
            return font_size_big

        # Tenta quebrar em 2 linhas pelo espaço mais próximo do meio
        words = text.split()
        best_split = 1
        best_diff = float("inf")
        for i in range(1, len(words)):
            l1 = " ".join(words[:i]); l2 = " ".join(words[i:])
            w1 = c.stringWidth(l1, font, font_size_big); w2 = c.stringWidth(l2, font, font_size_big)
            if w1 <= max_w and w2 <= max_w:
                diff = abs(w1 - w2)
                if diff < best_diff: best_diff = diff; best_split = i

        l1 = " ".join(words[:best_split]); l2 = " ".join(words[best_split:])
        w1 = c.stringWidth(l1, font, font_size_big); w2 = c.stringWidth(l2, font, font_size_big)

        if w1 <= max_w and w2 <= max_w:
            line_h = font_size_big + 1
            c.drawString(x, y + line_h / 2, l1)
            c.drawString(x, y + line_h / 2 - line_h, l2)
            return font_size_big

        # Se não coube em 2 linhas, reduz fonte progressivamente
        for fs in range(font_size_big - 1, 5, -1):
            c.setFont(font, fs)
            if c.stringWidth(text, font, fs) <= max_w:
                c.drawString(x, y, text)
                return fs

        # Último recurso: trunca
        c.setFont(font, 7)
        while c.stringWidth(text, font, 7) > max_w and len(text) > 4:
            text = text[:-1]
        c.drawString(x, y, text + ("…" if len(text) < len(text) else ""))
        return 7

    buf = io.BytesIO()
    c = rl_canvas.Canvas(buf, pagesize=letter)

    total = len(parts_sel)
    pages = _math.ceil(total / PER_PAGE) if total else 1

    for pg in range(pages):
        batch = parts_sel[pg * PER_PAGE:(pg + 1) * PER_PAGE]
        for idx, p in enumerate(batch):
            col = idx % COLS
            row = idx // COLS

            # Coordenada x (esquerda da etiqueta)
            x = MARGIN_LEFT + col * LBL_W
            # Coordenada y (base da etiqueta — ReportLab usa y de baixo pra cima)
            y = MARGIN_TOP - (row + 1) * LBL_H

            # Borda leve
            c.setStrokeColorRGB(0.80, 0.80, 0.80)
            c.setLineWidth(0.3)
            c.rect(x, y, LBL_W, LBL_H)

            # Área de texto com padding interno
            PAD = 6 * MM  # 6 mm de padding lateral
            txt_x = x + PAD
            txt_max_w = LBL_W - 2 * PAD

            if label_type == "nome":
                # ── Só nome — centralizado verticalmente ──────────────────
                name = p.get("Name", "")
                center_y = y + LBL_H / 2 - 5
                draw_name_wrapped(c, name, txt_x, center_y, txt_max_w, "Helvetica-Bold", 11, 9)

            elif label_type == "blusa":
                # ── Nome + Quarto + Blusa ─────────────────────────────────
                name = p.get("Name", "")
                quarto = pr.get(p["Id"], "-")
                shirt = SHIRT_MAP.get(p.get("ShirtSize", 0), "-")

                # Nome no topo
                name_y = y + LBL_H - 20
                draw_name_wrapped(c, name, txt_x, name_y, txt_max_w, "Helvetica-Bold", 10, 8)

                # Quarto e blusa na parte inferior
                c.setFont("Helvetica", 7.5)
                c.drawString(txt_x, y + 18, f"Quarto: {quarto}")
                c.drawString(txt_x, y + 7, f"Blusa: {shirt}")

            else:
                # ── Padrão: Nome + Categoria + Quarto ────────────────────
                name = p.get("Name", "")
                cat = p.get("Category") or "-"
                quarto = pr.get(p["Id"], "-")

                name_y = y + LBL_H - 20
                draw_name_wrapped(c, name, txt_x, name_y, txt_max_w, "Helvetica-Bold", 10, 8)

                c.setFont("Helvetica", 7.5)
                c.drawString(txt_x, y + 18, cat[:40])
                c.drawString(txt_x, y + 7, f"Quarto: {quarto}")

        if pg < pages - 1:
            c.showPage()

    c.save()
    return buf.getvalue()

def page_labels(eid, ev):
    st.markdown(f"## 🏷️ Etiquetas — {ev['Name']}")
    st.caption("Modelo: **Pimaco 6182** · Inkjet + Laser · Carta · 33,9 × 101,6 mm · **14 etiquetas/folha** (2 col × 7 lin)")
    parts = load_participants(eid); assigns = load_assignments(eid); rooms = load_rooms(eid)
    rm = {r["Id"]: r["Name"] for r in rooms}; pr = {a["ParticipantId"]: rm.get(a["RoomId"],"-") for a in assigns}

    # ── Filtros ──────────────────────────────────────────────────────────────
    f1, f2, f3 = st.columns(3)
    with f1: search_lbl = st.text_input("🔍 Buscar por nome", key="lbl_search")
    with f2: cat_lbl = st.selectbox("Categoria", ["Todos","Encontrista","Servo","Equipe"], key="lbl_cat")
    with f3:
        room_opts = ["Todos os quartos"] + sorted([r["Name"] for r in rooms])
        room_lbl = st.selectbox("🏠 Quarto", room_opts, key="lbl_room")

    filtered_lbl = parts
    if search_lbl: filtered_lbl = [p for p in filtered_lbl if search_lbl.lower() in p["Name"].lower()]
    if cat_lbl == "Encontrista": filtered_lbl = [p for p in filtered_lbl if is_encounterist(p.get("Category"))]
    elif cat_lbl == "Servo": filtered_lbl = [p for p in filtered_lbl if is_server(p.get("Category"))]
    elif cat_lbl == "Equipe": filtered_lbl = [p for p in filtered_lbl if norm(p.get("Category","")).startswith("equipe")]
    if room_lbl != "Todos os quartos":
        filtered_lbl = [p for p in filtered_lbl if pr.get(p["Id"], "-") == room_lbl]

    # ── Repetição em sequência ────────────────────────────────────────────────
    repeat_qty = st.number_input(
        "🔁 Cópias em sequência por pessoa",
        min_value=1, max_value=20, value=1, step=1, key="lbl_repeat",
        help="Ex: 3 = João João João Maria Maria Maria"
    )

    flag_key = f"lbl_flags_{eid}"

    # ── Seleção via multiselect nativo ────────────────────────────────────────
    nomes_filtrados = [p["Name"] for p in filtered_lbl]
    # Preserva seleção anterior que ainda existe na lista filtrada
    prev_sel = st.session_state.get(flag_key + "_names", nomes_filtrados)
    prev_sel_valid = [n for n in prev_sel if n in nomes_filtrados]

    selected_names = st.multiselect(
        "Selecionar participantes para imprimir (vazio = todos)",
        options=nomes_filtrados,
        default=prev_sel_valid,
        key="lbl_multisel",
        placeholder="Selecione nomes ou deixe vazio para imprimir todos..."
    )
    st.session_state[flag_key + "_names"] = selected_names

    # Monta lista base
    name_set = set(selected_names)
    base_list = [p for p in filtered_lbl if p["Name"] in name_set] if selected_names else filtered_lbl
    parts_to_print = [p for p in base_list for _ in range(int(repeat_qty))]
    n_etiquetas = len(parts_to_print)
    n_folhas = (n_etiquetas + 13) // 14
    sel_label = f"✔ {len(base_list)} selecionado(s)" if selected_names else f"Todos ({len(filtered_lbl)})"

    if repeat_qty > 1:
        st.info(f"📄 **{len(base_list)}** pessoa(s) × **{repeat_qty}** cópias = **{n_etiquetas}** etiqueta(s) · **{n_folhas}** folha(s) · {sel_label}")
    else:
        st.info(f"📄 **{n_etiquetas}** etiqueta(s) · **{n_folhas}** folha(s) · {sel_label}")

    # ── Botões de download ────────────────────────────────────────────────────
    b1, b2 = st.columns(2)
    with b1:
        if st.button("🖨️ Gerar etiqueta camisa (nome + quarto + blusa)", type="primary", use_container_width=True, key="btn_camisa"):
            with st.spinner("Gerando PDF..."):
                pdf = generate_labels_pimaco(parts_to_print, "blusa", assigns, rooms)
                st.download_button("⬇️ Baixar PDF Camisa", pdf, "etiquetas_camisa_6182.pdf", "application/pdf", use_container_width=True)
    with b2:
        if st.button("🖨️ Gerar etiqueta só nome", use_container_width=True, key="btn_nome"):
            with st.spinner("Gerando PDF..."):
                pdf = generate_labels_pimaco(parts_to_print, "nome", assigns, rooms)
                st.download_button("⬇️ Baixar PDF Nome", pdf, "etiquetas_nome_6182.pdf", "application/pdf", use_container_width=True)

    # ── Preview da lista selecionada ──────────────────────────────────────────
    st.divider()
    st.markdown(f"**{len(filtered_lbl)} participante(s) disponíveis**")
    for p in filtered_lbl:
        pid = p["Id"]
        marcado = p["Name"] in name_set if selected_names else True
        quarto = pr.get(pid, "-"); shirt = SHIRT_MAP.get(p.get("ShirtSize",0),"-"); cat = p.get("Category") or "-"
        icon = "✅" if marcado else "⬜"
        st.markdown(f"{icon} **{p['Name']}**")
        st.caption(f"{cat} · Quarto: {quarto} · Camisa: {shirt}")

def make_gdrive_view_url(raw_url):
    """
    Converte qualquer link do Google Drive para URL de visualização direta no navegador.
    Suporta: /open?id=, /file/d/, /uc?id=, ID puro.
    """
    if not raw_url: return None
    raw_url = raw_url.strip()
    # Tenta extrair o file ID
    fid = None
    # /open?id=XXX  ou  /uc?id=XXX  ou  ?id=XXX
    m = re.search(r"[?&]id=([A-Za-z0-9\-_]+)", raw_url, re.I)
    if m: fid = m.group(1)
    # /file/d/XXX/
    if not fid:
        m = re.search(r"/d/([A-Za-z0-9\-_]+)", raw_url, re.I)
        if m: fid = m.group(1)
    # ID puro
    if not fid and re.match(r"^[A-Za-z0-9\-_]{20,}$", raw_url):
        fid = raw_url
    if fid:
        return f"https://drive.google.com/file/d/{fid}/view"
    return raw_url  # devolve original se não reconheceu

def page_photos(eid, ev):
    st.markdown(f"## 📸 Fotos — {ev['Name']}")
    url = ev.get("PhotosSheetUrl") or ""
    parts = load_participants(eid); enc = [p for p in parts if is_encounterist(p.get("Category"))]

    if not url:
        st.warning("⚠️ URL da planilha de fotos não configurada. Acesse **Configurações** para adicionar.")
        return

    dl_key = f"photos_dl_{eid}"; prev_key = f"photos_prev_count_{eid}"
    dl_info = st.session_state.get(dl_key)
    if dl_info:
        prev_count = st.session_state.get(prev_key, dl_info["total"])
        new_count = dl_info["total"]
        delta_str = f" · **+{new_count - prev_count} nova(s)**" if new_count > prev_count else ""
        st.info(f"📥 Último carregamento: **{dl_info['ts']}** · {new_count} foto(s) mapeadas{delta_str}")

    if st.button("🔄 Atualizar", type="primary"):
        with st.spinner("Lendo planilha..."):
            try:
                text = fetch_sheet_csv(url)
                df = pd.read_csv(io.StringIO(text), dtype=str); df.columns = [c.strip() for c in df.columns]
                # Colunas do encontrista — inclui "Nome do Encontrista" e variações do formulário
                cn = find_header(df.columns, [
                    "Nome do Encontrista", "Nome do encontrista:", "Nome do encontrista",
                    "Encontrista", "Nome"
                ])
                # Colunas de foto — inclui "Foto" e variações
                cp = find_header(df.columns, [
                    "Foto", "Fotos", "URL da Foto", "URL das Fotos", "URL", "Link"
                ])
                if not cn: st.error(f"Coluna 'Nome do Encontrista' não encontrada. Colunas: {', '.join(df.columns)}"); return
                if not cp: st.error(f"Coluna 'Foto' não encontrada. Colunas: {', '.join(df.columns)}"); return
                groups = {}
                for _, row in df.iterrows():
                    name = safe_str(row.get(cn)); photo = safe_str(row.get(cp))
                    if name and photo:
                        groups.setdefault(name, [])
                        for lk in photo.split(","):
                            lk = lk.strip()
                            if lk: groups[name].append(lk)
                total = sum(len(v) for v in groups.values())
                prev = st.session_state.get(dl_key, {}).get("total", total)
                st.session_state[prev_key] = prev
                st.session_state[dl_key] = {"ts": datetime.now().strftime("%d/%m/%Y às %H:%M"), "total": total}
                st.session_state[f"photo_groups_{eid}"] = groups
                st.session_state[f"photos_loaded_{eid}"] = True
                st.success(f"✅ {len(groups)} encontrista(s) com fotos — {total} foto(s) mapeadas.")
            except Exception as e:
                st.error(f"Erro ao carregar planilha: {e}")
                st.caption("💡 Verifique se a planilha está compartilhada como 'Qualquer pessoa com o link pode ver'.")

    groups = st.session_state.get(f"photo_groups_{eid}", {})
    loaded = st.session_state.get(f"photos_loaded_{eid}", False)
    sec_status = st.session_state.get(f"sec_status_{eid}", {})

    st.divider()
    sem_foto = sum(1 for p in enc if not any(
        norm(p["Name"])==norm(n) or p["Name"].lower() in n.lower() or n.lower() in p["Name"].lower()
        for n in groups))
    st.markdown(f"**{len(enc)} encontrista(s)** · {sem_foto} sem foto mapeada")

    search_f = st.text_input("🔍 Filtrar por nome", key="fto_search")
    enc_filtrado = [p for p in enc if search_f.lower() in p["Name"].lower()] if search_f else enc

    for p in enc_filtrado:
        pn = p["Name"]; links = []
        # Coleta todos os links deste encontrista (pode estar em várias linhas da planilha)
        for name, lks in groups.items():
            if norm(name)==norm(pn) or pn.lower() in name.lower() or name.lower() in pn.lower():
                links.extend(lks)
        count = len(links)
        finalizado = sec_status.get(p["Id"], {}).get("bolsa_ok", False)
        label_extra = " — FINALIZADO ✅" if finalizado else ""
        icon = "✅" if finalizado else ("📷" if count > 0 else "🚨")

        with st.expander(f"{icon} {pn} — {count} foto(s){label_extra}"):
            if finalizado:
                st.success("Bolsa finalizada pela Secretária.")
            if links:
                st.caption("Clique nos links abaixo para abrir cada foto no navegador e fazer o download:")
                # Converte cada link para URL de visualização e exibe
                for i, lk in enumerate(links, 1):
                    view_url = make_gdrive_view_url(lk)
                    if view_url:
                        st.markdown(f"📷 [Foto {i} — abrir no navegador]({view_url})")
                    else:
                        st.markdown(f"📷 Foto {i}: `{lk}`")
            else:
                if loaded:
                    st.warning("Sem fotos recebidas para este encontrista.")
                else:
                    st.caption("Clique em Atualizar para carregar a planilha.")

def page_secretary(eid, ev):
    st.markdown(f"## 🗂️ Secretária — {ev['Name']}")
    st.caption("Controle de montagem das bolsas: cartas e fotos por encontrista.")

    parts = load_participants(eid); assigns = load_assignments(eid); rooms = load_rooms(eid)
    enc = [p for p in parts if is_encounterist(p.get("Category"))]
    rm = {r["Id"]: r["Name"] for r in rooms}
    pr = {a["ParticipantId"]: rm.get(a["RoomId"], "-") for a in assigns}

    team_key = f"sec_team_{eid}"
    dist_key = f"sec_dist_{eid}"
    status_key = f"sec_status_{eid}"

    if team_key not in st.session_state:
        db_team, db_dist, db_status = load_secretary_state(eid)
        st.session_state[team_key] = db_team
        st.session_state[dist_key] = db_dist
        st.session_state[status_key] = db_status

    team: list = st.session_state[team_key]
    dist: dict = st.session_state[dist_key]
    sec_status: dict = st.session_state[status_key]

    def persist():
        save_secretary_state(eid, team, dist, sec_status)

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

    tab1, tab2, tab3 = st.tabs(["👥 Equipe", "🔀 Distribuição", "📋 Acompanhamento"])

    with tab1:
        st.markdown("#### Adicionar membro da equipe")
        servos = [p for p in parts if is_server(p.get("Category"))]
        servo_names = [p["Name"] for p in servos]
        novo_membro = st.selectbox("Selecionar servo", [""] + servo_names, key="sec_sel_servo")
        custom_membro = st.text_input("Ou digitar nome manualmente", key="sec_custom")
        if st.button("➕ Adicionar à equipe"):
            nome = custom_membro.strip() or novo_membro
            if nome and nome not in team:
                team.append(nome); st.session_state[team_key] = team
                persist(); st.rerun()
            elif nome in team:
                st.warning(f"'{nome}' já está na equipe.")
        st.divider()
        st.markdown(f"#### Equipe atual ({len(team)} pessoas)")
        if not team:
            st.info("Nenhum membro adicionado ainda.")
        else:
            for i, m in enumerate(team):
                col_m1, col_m2 = st.columns([5,1])
                with col_m1: st.markdown(f"👤 **{m}**")
                with col_m2:
                    if st.button("✕", key=f"rm_team_{i}"):
                        team.pop(i); st.session_state[team_key] = team
                        if m in dist: del dist[m]; st.session_state[dist_key] = dist
                        persist(); st.rerun()

    with tab2:
        if not team:
            st.warning("Adicione membros na aba Equipe primeiro.")
        elif not enc:
            st.warning("Nenhum encontrista cadastrado.")
        else:
            st.markdown(f"**{len(enc)} encontristas** para distribuir entre **{len(team)} pessoas**")
            st.caption("A distribuição é feita por quarto, igualmente entre os membros.")
            if st.button("🔀 Distribuir encontristas", type="primary"):
                by_room = {}
                for p in enc:
                    quarto = pr.get(p["Id"], "Sem quarto")
                    by_room.setdefault(quarto, []).append(p)
                ordered = []
                for quarto_nome in sorted(by_room.keys()):
                    ordered.extend(by_room[quarto_nome])
                new_dist = {m: [] for m in team}
                for idx, p in enumerate(ordered):
                    membro = team[idx % len(team)]
                    new_dist[membro].append(p["Id"])
                st.session_state[dist_key] = new_dist; dist = new_dist
                persist(); st.success("✅ Distribuição feita!"); st.rerun()

            if dist:
                todos_dist = {pid for pids in dist.values() for pid in pids}
                sem_dist = [p for p in enc if p["Id"] not in todos_dist]
                if sem_dist:
                    st.warning(f"⚠️ **{len(sem_dist)} encontrista(s) sem distribuição** — clique em Distribuir para incluir:")
                    for p in sem_dist: st.caption(f"  • {p['Name']}")
                st.divider()
                st.markdown("#### Resultado da distribuição")
                for membro, pids in dist.items():
                    people = [p for p in enc if p["Id"] in pids]
                    st.markdown(f"**👤 {membro}** — {len(people)} encontrista(s)")
                    rows_d = [{"Nome": p["Name"], "Quarto": pr.get(p["Id"],"-"), "Indicado por": p.get("InvitedBy") or "-"} for p in people]
                    if rows_d: st.dataframe(pd.DataFrame(rows_d), hide_index=True, use_container_width=True)
                    st.divider()

    with tab3:
        if not dist:
            st.info("Faça a distribuição primeiro na aba Distribuição.")
        else:
            membro_sel = st.selectbox("👤 Ver acompanhamento de:", list(dist.keys()), key="sec_member_sel")
            pids_sel = dist.get(membro_sel, [])
            people_sel = [p for p in enc if p["Id"] in pids_sel]

            total = len(people_sel)
            finalizados = sum(1 for p in people_sel if sec_status.get(p["Id"],{}).get("bolsa_ok",False))
            st.markdown(f"**{finalizados}/{total}** bolsas finalizadas")
            st.progress(finalizados/total if total else 0)
            st.divider()

            for p in people_sel:
                pid = p["Id"]
                ps = sec_status.get(pid, {})
                bolsa_ok = ps.get("bolsa_ok", False)
                cartas_ok = ps.get("cartas_ok", False)
                fotos_ok = ps.get("fotos_ok", False)
                n_cartas = count_letters(p["Name"])
                n_fotos = count_photos(p["Name"])
                quarto = pr.get(pid, "-")
                indicado = p.get("InvitedBy") or "-"
                icon = "✅" if bolsa_ok else ("🟡" if (cartas_ok or fotos_ok) else "⬜")
                with st.container(border=True):
                    r1, r2 = st.columns([4,2])
                    with r1:
                        st.markdown(f"{icon} **{p['Name']}**")
                        st.caption(f"Quarto: {quarto} · Indicado por: {indicado}")
                        sub1, sub2 = st.columns(2)
                        with sub1:
                            c_icon = "✅" if cartas_ok else ("📭" if n_cartas == 0 else "📬")
                            st.markdown(f"{c_icon} **Cartas:** {n_cartas}")
                            if not bolsa_ok:
                                new_cartas = st.checkbox("Cartas OK", value=cartas_ok, key=f"ck_{pid}")
                                if new_cartas != cartas_ok:
                                    if pid not in sec_status: sec_status[pid] = {}
                                    sec_status[pid]["cartas_ok"] = new_cartas; st.session_state[status_key] = sec_status; persist(); st.rerun()
                        with sub2:
                            f_icon = "✅" if fotos_ok else ("📭" if n_fotos == 0 else "📷")
                            st.markdown(f"{f_icon} **Fotos:** {n_fotos}")
                            if not bolsa_ok:
                                new_fotos = st.checkbox("Fotos OK", value=fotos_ok, key=f"fk_{pid}")
                                if new_fotos != fotos_ok:
                                    if pid not in sec_status: sec_status[pid] = {}
                                    sec_status[pid]["fotos_ok"] = new_fotos; st.session_state[status_key] = sec_status; persist(); st.rerun()
                    with r2:
                        if bolsa_ok:
                            st.success("✅ Bolsa Finalizada")
                            if st.button("↩️ Reabrir", key=f"reopen_{pid}"):
                                sec_status[pid]["bolsa_ok"] = False; st.session_state[status_key] = sec_status; persist(); st.rerun()
                        else:
                            if st.button("✅ Bolsa Finalizada", key=f"done_{pid}", type="primary", use_container_width=True):
                                if pid not in sec_status: sec_status[pid] = {}
                                sec_status[pid]["bolsa_ok"] = True; sec_status[pid]["cartas_ok"] = True; sec_status[pid]["fotos_ok"] = True
                                st.session_state[status_key] = sec_status; persist(); st.rerun()

def page_settings(eid, ev):
    st.markdown(f"## ⚙️ Config — {ev['Name']}")
    with st.form("cfg"):
        name = st.text_input("Nome", value=ev.get("Name",""))
        c1,c2 = st.columns(2)
        with c1: sd = st.date_input("Início", value=date.fromisoformat(ev["StartDate"]) if ev.get("StartDate") else date.today())
        with c2: ed = st.date_input("Fim", value=date.fromisoformat(ev["EndDate"]) if ev.get("EndDate") else date.today())
        loc = st.text_input("Local", value=ev.get("Location") or "")
        sts = ["Config","Aberto","Em andamento","Encerrado"]; cs = ev.get("Status") or "Config"
        status = st.selectbox("Status", sts, index=sts.index(cs) if cs in sts else 0)
        st.divider(); st.markdown("**Planilhas**")
        ru = st.text_input("Inscrições", value=ev.get("RegistrationSheetUrl") or "")
        lu = st.text_input("Cartas", value=ev.get("LettersSheetUrl") or "")
        phu = st.text_input("Fotos", value=ev.get("PhotosSheetUrl") or "")
        if st.form_submit_button("Salvar", type="primary"):
            get_sb().table("Events").update({"Name":name,"StartDate":sd.isoformat(),"EndDate":ed.isoformat(),"Location":loc or None,"Status":status,"RegistrationSheetUrl":ru or None,"LettersSheetUrl":lu or None,"PhotosSheetUrl":phu or None,"UpdatedAtUtc":utcnow()}).eq("Id",eid).execute()
            st.success("Salvo!"); st.rerun()

# ═══════════════════════════════════════════════════════════════════════════════
def main():
    st.set_page_config(page_title="Gestão Encontro com Deus", page_icon="⛪", layout="wide", initial_sidebar_state="expanded")
    if "authenticated" not in st.session_state: st.session_state.authenticated = False
    if not st.session_state.authenticated: show_login(); return
    with st.sidebar:
        st.markdown("### ⛪ Encontro com Deus"); st.divider()
        if st.button("🚪 Sair", use_container_width=True): st.session_state.authenticated = False; st.session_state.pop("current_event", None); st.rerun()
    if "page" not in st.session_state: st.session_state.page = "events"
    page = st.session_state.page; eid = st.session_state.get("current_event")
    if page == "event_new": page_event_new(); return
    if page == "events" or not eid: page_events(); return
    ev = event_sidebar(eid)
    if not ev: return
    {"dashboard":page_dashboard,"participants":page_participants,"rooms":page_rooms,"schedule":page_schedule,"letters":page_letters,"labels":page_labels,"photos":page_photos,"secretary":page_secretary,"settings":page_settings}.get(page, page_dashboard)(eid, ev)

if __name__ == "__main__": main()
