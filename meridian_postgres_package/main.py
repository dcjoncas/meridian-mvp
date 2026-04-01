import os, io, re, html, hashlib, random, secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import Json, RealDictCursor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv("DATABASE_URL")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required for the Postgres build.")
pool = SimpleConnectionPool(1, 10, DATABASE_URL)

def get_conn():
    return pool.getconn()

def put_conn(conn):
    pool.putconn(conn)

def make_gmid(seed: str) -> str:
    return hashlib.sha256((seed + "|MERIDIAN_PG_V1").encode("utf-8")).hexdigest()

def tokenize(text: str) -> List[str]:
    return [t.lower() for t in re.findall(r"[a-zA-Z0-9&\-/]+", text or "") if len(t) > 1]

def normalize_list(value):
    if value is None:
        return []
    items = value if isinstance(value, list) else [x.strip() for x in str(value).split(",")]
    out, seen = [], set()
    for item in items:
        s = str(item).strip()
        if s and s.lower() not in seen:
            out.append(s)
            seen.add(s.lower())
    return out

def profile_strength_score(p: Dict[str, Any]) -> int:
    score = 0
    score += min(18, len(p.get("domains", [])) * 4)
    score += min(18, len(p.get("roles", [])) * 5)
    score += min(14, int(p.get("experience_years") or 0))
    score += min(14, len(p.get("networks", [])) * 4)
    score += min(20, len(p.get("assets", [])) * 2)
    score += min(8, len(p.get("values", [])) * 2)
    attrs = p.get("attributes") or {}
    if isinstance(attrs, dict):
        score += min(8, len([k for k,v in attrs.items() if v not in (None, "", [], {})]))
    return min(100, score)

def weighted_hits(query: str, items: List[str], exact_weight: int, token_weight: int) -> int:
    q = query.lower(); q_tokens = set(tokenize(query)); score = 0
    for item in items:
        s = str(item).lower()
        if s in q or q in s:
            score += exact_weight
        else:
            score += len(set(tokenize(s)) & q_tokens) * token_weight
    return score

def extract_years_from_query(query: str):
    m = re.search(r"(\d+)\+?\s+years?", query.lower())
    return int(m.group(1)) if m else None

def score_profile(query: str, p: Dict[str, Any]) -> int:
    if not query.strip(): return 0
    score = 0
    score += weighted_hits(query, p.get("domains", []), 18, 6)
    score += weighted_hits(query, p.get("roles", []), 16, 5)
    score += weighted_hits(query, p.get("networks", []), 10, 3)
    score += weighted_hits(query, p.get("values", []), 7, 2)
    score += weighted_hits(query, (p.get("assets") or [])[:20], 4, 1)
    exp = int(p.get("experience_years") or 0)
    wanted = extract_years_from_query(query)
    score += max(0, 14 - min(14, abs(exp - wanted))) if wanted is not None else min(8, exp // 2)
    score += profile_strength_score(p) * 0.12
    tie = int(hashlib.sha256((query + "|" + p["gmid"]).encode("utf-8")).hexdigest()[:2], 16) / 255.0
    score += tie * 1.9
    return max(0, min(100, int(round(score))))

DOMAIN_KEYWORDS = ["Manufacturing","Aerospace","Defense","Semiconductors","Automotive","Industrial Automation","Supply Chain","Logistics","Procurement","Quality Systems","Lean / Six Sigma","Plant Ops","Construction","Energy","Renewables","Utilities","Oil & Gas","Healthcare","Financial Services","Private Equity","Venture Capital","Cybersecurity","Cloud Infrastructure","Data Platforms","AI/ML","ERP Transformations","Program Delivery","Compliance","Risk & Controls","Executive Search","Strategic Introductions"]
ROLE_KEYWORDS = ["COO","CTO","VP Manufacturing","Plant Director","Director of Operations","Head of Supply Chain","Head of Procurement","Quality Director","Transformation Lead","Operating Advisor","Program Executive","CISO","Data Platform Lead","Integration Architect","Finance Transformation Lead","Principal","Managing Partner"]
NETWORK_KEYWORDS = ["Global LP network","C-suite operator channel","Board-level advisory","Fortune 500 operator network","Global tier-1 suppliers","PE operating partners","OEM executive channel","Cloud provider exec channel","Defense-industrial base partners","Board network","Investor network"]
VALUE_KEYWORDS = ["Discretion","Reciprocity","Outcome rigor","Trust","Non-attribution","Calm under pressure","Zero-ego execution"]

def extract_text_from_upload(upload: UploadFile, raw: bytes) -> str:
    name = (upload.filename or "").lower()
    if name.endswith((".txt", ".md", ".csv", ".json")):
        return raw.decode("utf-8", errors="ignore")
    if name.endswith((".html", ".htm")):
        txt = raw.decode("utf-8", errors="ignore")
        txt = re.sub(r"<script.*?</script>", " ", txt, flags=re.S|re.I)
        txt = re.sub(r"<style.*?</style>", " ", txt, flags=re.S|re.I)
        txt = re.sub(r"<[^>]+>", " ", txt)
        return html.unescape(txt)
    if name.endswith(".docx"):
        try:
            from docx import Document
            doc = Document(io.BytesIO(raw))
            return "\n".join(p.text for p in doc.paragraphs)
        except Exception:
            return raw.decode("utf-8", errors="ignore")
    if name.endswith(".pdf"):
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(raw))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception:
            return ""
    return raw.decode("utf-8", errors="ignore")

def parse_profile_text(text: str) -> Dict[str, Any]:
    lower = (text or "").lower()
    domains = [d for d in DOMAIN_KEYWORDS if d.lower() in lower]
    roles = [r for r in ROLE_KEYWORDS if r.lower() in lower]
    networks = [n for n in NETWORK_KEYWORDS if n.lower() in lower]
    values = [v for v in VALUE_KEYWORDS if v.lower() in lower]
    years = 0
    matches = re.findall(r"(\d+)\+?\s+years?", lower)
    if matches: years = max(int(x) for x in matches)
    lines = [re.sub(r"\s+", " ", x).strip(" -•\t") for x in re.split(r"[\n\r]+|[;]", text)]
    assets = []
    for line in lines:
        if len(line) < 18: continue
        if any(w in line.lower() for w in ["led","built","delivered","improved","implemented","executed","created","managed","launched","reduced","increased","designed","negotiated","supported","stabilized"]):
            if line not in assets: assets.append(line)
        if len(assets) >= 12: break
    attributes = {}
    for key in ["sap","oracle","azure","aws","board","global","confidential"]:
        if key in lower: attributes[key] = True
    return {"domains": domains, "roles": roles, "networks": networks, "values": values, "assets": assets, "experience_years": years, "attributes": attributes, "extracted_preview": text[:1200]}

def init_schema():
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS members (id BIGSERIAL PRIMARY KEY, gmid TEXT UNIQUE NOT NULL, display_name TEXT NOT NULL, email TEXT, is_system BOOLEAN NOT NULL DEFAULT FALSE, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS member_profiles (member_id BIGINT PRIMARY KEY REFERENCES members(id) ON DELETE CASCADE, headline TEXT, biography TEXT, domains_json JSONB NOT NULL DEFAULT '[]'::jsonb, roles_json JSONB NOT NULL DEFAULT '[]'::jsonb, experience_years INT NOT NULL DEFAULT 0, networks_json JSONB NOT NULL DEFAULT '[]'::jsonb, political_social_json JSONB NOT NULL DEFAULT '[]'::jsonb, assets_json JSONB NOT NULL DEFAULT '[]'::jsonb, values_json JSONB NOT NULL DEFAULT '[]'::jsonb, attributes_json JSONB NOT NULL DEFAULT '{}'::jsonb, strength_score INT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS member_documents (id BIGSERIAL PRIMARY KEY, member_id BIGINT NOT NULL REFERENCES members(id) ON DELETE CASCADE, filename TEXT NOT NULL, content_type TEXT, extracted_text TEXT, parsed_json JSONB NOT NULL DEFAULT '{}'::jsonb, uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS member_invitations (id BIGSERIAL PRIMARY KEY, candidate_name TEXT NOT NULL, candidate_email TEXT NOT NULL, reference_gmid TEXT NOT NULL, invited_by_gmid TEXT NOT NULL, invitation_token TEXT UNIQUE NOT NULL, invitation_status TEXT NOT NULL DEFAULT 'sent', invite_note TEXT, sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), accepted_at TIMESTAMPTZ, expires_at TIMESTAMPTZ NOT NULL);
            CREATE TABLE IF NOT EXISTS member_references (id BIGSERIAL PRIMARY KEY, invitation_id BIGINT NOT NULL REFERENCES member_invitations(id) ON DELETE CASCADE, reference_gmid TEXT NOT NULL, sponsor_gmid TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
            CREATE TABLE IF NOT EXISTS pings (id BIGSERIAL PRIMARY KEY, requester_gmid TEXT NOT NULL, target_gmid TEXT NOT NULL, request_text TEXT NOT NULL, score INT NOT NULL, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), responded_at TIMESTAMPTZ);
            CREATE TABLE IF NOT EXISTS chat_messages (id BIGSERIAL PRIMARY KEY, ping_id BIGINT NOT NULL REFERENCES pings(id) ON DELETE CASCADE, sender_gmid TEXT NOT NULL, message TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
            """)
            cur.execute("SELECT id FROM members WHERE display_name=%s", ("Mike S",))
            if not cur.fetchone():
                gmid = make_gmid("Mike S|PRINCIPAL")
                cur.execute("INSERT INTO members (gmid, display_name, email, is_system, status) VALUES (%s,%s,%s,%s,%s) RETURNING id", (gmid, "Mike S", "mike@meridian.local", False, "active"))
                member_id = cur.fetchone()[0]
                profile = {"domains": ["Private Equity", "Financial Services", "Executive Search", "Strategic Introductions"], "roles": ["Principal", "Managing Partner"], "experience_years": 18, "networks": ["Global LP network", "C-suite operator channel", "Board-level advisory"], "assets": ["20+ years executing discreet executive mandates","Deep LP and sovereign fund relationships","Multi-sector board and operating network","Cross-border deal origination track record","Non-attributable introduction protocol","Known for zero-ego, outcome-first execution"], "values": ["Discretion","Reciprocity","Outcome rigor"], "attributes": {"engagement_type":"advisory","confidentiality":"non-attribution"}}
                cur.execute("INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (member_id, Json(profile["domains"]), Json(profile["roles"]), profile["experience_years"], Json(profile["networks"]), Json(profile["assets"]), Json(profile["values"]), Json(profile["attributes"]), profile_strength_score(profile)))
            cur.execute("SELECT COUNT(*) FROM members WHERE is_system = TRUE")
            if cur.fetchone()[0] == 0:
                first = ["Avery","Jordan","Riley","Casey","Morgan","Taylor","Quinn","Hayden","Parker","Rowan","Blake","Cameron","Drew","Emerson","Finley","Harper","Kai","Logan","Micah","Noel"]
                last  = ["Stone","Reed","Carter","Hayes","Brooks","Wells","Foster","Shaw","Bennett","Cole","Sullivan","Pierce","Vaughn","Donovan","Holland","Walsh","Hayward","Monroe","Kendall","Navarro"]
                for i in range(120):
                    rnd = random.Random(i + 77)
                    display = f"{first[i % len(first)]} {last[(i*3) % len(last)]} — EX-{i+1:03d}"
                    gmid = make_gmid("SYSTEM|" + display)
                    cur.execute("INSERT INTO members (gmid, display_name, is_system, status) VALUES (%s,%s,%s,%s) RETURNING id", (gmid, display, True, "active"))
                    member_id = cur.fetchone()[0]
                    domains = rnd.sample(DOMAIN_KEYWORDS[:24], k=rnd.randint(2, 4))
                    roles = rnd.sample(ROLE_KEYWORDS[:15], k=rnd.randint(1, 2))
                    networks = rnd.sample(NETWORK_KEYWORDS, k=rnd.randint(1, 3))
                    assets = rnd.sample(["Ran a multi-site manufacturing turnaround","Commissioned a greenfield plant","Reduced scrap by double-digits","Improved OEE by 10+ points","Implemented a tiered daily management system","Built a best-in-class maintenance program","Established a robust supplier quality program","Led a major supplier renegotiation","Built S&OP cadence and governance","Reduced inventory without hurting service","Delivered ERP fit-to-standard with minimal custom","Cut over a complex ERP deployment","Led CPI/integration modernization","Implemented zero-trust segmentation","Executed a carve-out / TSA separation","Supported a post-merger integration","Executed a cost takeout program","Negotiated strategic long-term supply agreements","Improved safety performance","Improved first-pass yield","Introduced standard work and coaching","Executed international expansion"], k=rnd.randint(8, 14))
                    values = rnd.sample(VALUE_KEYWORDS, k=rnd.randint(2, 4))
                    profile = {"domains": domains, "roles": roles, "experience_years": rnd.randint(8, 28), "networks": networks, "assets": assets, "values": values, "attributes": {"engagement_type":"advisory retainer","availability":"near-term"}}
                    cur.execute("INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (member_id, Json(profile["domains"]), Json(profile["roles"]), profile["experience_years"], Json(profile["networks"]), Json(profile["assets"]), Json(profile["values"]), Json(profile["attributes"]), profile_strength_score(profile)))
    finally:
        put_conn(conn)

app = FastAPI(title="Meridian Postgres", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
@app.on_event("startup")
def startup(): init_schema()

def fetch_profiles(limit: int = 250):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT m.gmid, m.display_name, m.is_system, m.status, m.created_at, p.domains_json AS domains, p.roles_json AS roles, p.experience_years, p.networks_json AS networks, p.political_social_json AS political_social, p.assets_json AS assets, p.values_json AS values, p.attributes_json AS attributes, p.strength_score FROM members m JOIN member_profiles p ON p.member_id = m.id ORDER BY m.created_at DESC LIMIT %s", (limit,))
            return cur.fetchall()
    finally:
        put_conn(conn)

@app.get("/", response_class=HTMLResponse)
def home(): return HTMLResponse(open(os.path.join(BASE_DIR, "ui.html"), "r", encoding="utf-8").read())
@app.get("/member/{gmid}", response_class=HTMLResponse)
def member(gmid: str): return HTMLResponse(open(os.path.join(BASE_DIR, "member.html"), "r", encoding="utf-8").read().replace("{{GMID}}", gmid))
@app.get("/rankings", response_class=HTMLResponse)
def rankings_page(): return HTMLResponse(open(os.path.join(BASE_DIR, "rankings.html"), "r", encoding="utf-8").read())
@app.get("/alias", response_class=HTMLResponse)
def alias_page(): return HTMLResponse(open(os.path.join(BASE_DIR, "GMID.html"), "r", encoding="utf-8").read())
@app.get("/invite-member", response_class=HTMLResponse)
def invite_member_page(): return HTMLResponse(open(os.path.join(BASE_DIR, "invite_member.html"), "r", encoding="utf-8").read())
@app.get("/invite/{token}", response_class=HTMLResponse)
def complete_profile_page(token: str): return HTMLResponse(open(os.path.join(BASE_DIR, "complete_profile.html"), "r", encoding="utf-8").read().replace("{{TOKEN}}", token))
@app.get("/api/profiles")
def api_profiles(limit: int = 250): return JSONResponse({"count": limit, "profiles": fetch_profiles(limit)})
@app.get("/api/profile_summary/{gmid}")
def api_profile_summary(gmid: str):
    profiles = fetch_profiles(5000)
    current = next((p for p in profiles if p["gmid"] == gmid), None)
    if not current: raise HTTPException(status_code=404, detail="profile not found")
    scored = sorted([{"gmid": p["gmid"], "score": profile_strength_score(p)} for p in profiles], key=lambda x: x["score"], reverse=True)
    rank = next((i + 1 for i, row in enumerate(scored) if row["gmid"] == gmid), None)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM pings WHERE status='accepted' AND (requester_gmid=%s OR target_gmid=%s)", (gmid, gmid))
            total_connections = cur.fetchone()["c"]
    finally:
        put_conn(conn)
    return JSONResponse({"ok": True, "rank": rank, "total_profiles": len(scored), "strength_score": profile_strength_score(current), "total_connections": total_connections, "profile": current})
@app.post("/api/profile/{gmid}/update")
async def api_profile_update(gmid: str, request: Request):
    data = await request.json(); conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT m.id AS member_id, p.domains_json AS domains, p.roles_json AS roles, p.experience_years, p.networks_json AS networks, p.assets_json AS assets, p.values_json AS values, p.attributes_json AS attributes FROM members m JOIN member_profiles p ON p.member_id = m.id WHERE m.gmid=%s", (gmid,))
            row = cur.fetchone()
            if not row: raise HTTPException(status_code=404, detail="profile not found")
            domains = normalize_list(data.get("domains")) or row["domains"]; roles = normalize_list(data.get("roles")) or row["roles"]; networks = normalize_list(data.get("networks")) or row["networks"]; assets = normalize_list(data.get("assets")) or row["assets"]; values = normalize_list(data.get("values")) or row["values"]; experience_years = int(data.get("experience_years") if data.get("experience_years") not in (None, "") else row["experience_years"]); attributes = row["attributes"] or {}
            profile = {"domains": domains, "roles": roles, "experience_years": experience_years, "networks": networks, "assets": assets, "values": values, "attributes": attributes}
            cur.execute("UPDATE member_profiles SET domains_json=%s, roles_json=%s, experience_years=%s, networks_json=%s, assets_json=%s, values_json=%s, attributes_json=%s, strength_score=%s, updated_at=NOW() WHERE member_id=%s", (Json(domains), Json(roles), experience_years, Json(networks), Json(assets), Json(values), Json(attributes), profile_strength_score(profile), row["member_id"]))
        return JSONResponse({"ok": True})
    finally:
        put_conn(conn)
@app.post("/api/profile/{gmid}/ingest-document")
async def api_profile_ingest(gmid: str, file: UploadFile = File(...)):
    raw = await file.read(); text = extract_text_from_upload(file, raw)
    if not text.strip(): raise HTTPException(status_code=400, detail="Could not extract readable text from the uploaded document.")
    extracted = parse_profile_text(text); conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT m.id AS member_id, p.domains_json AS domains, p.roles_json AS roles, p.experience_years, p.networks_json AS networks, p.assets_json AS assets, p.values_json AS values, p.attributes_json AS attributes FROM members m JOIN member_profiles p ON p.member_id = m.id WHERE m.gmid=%s", (gmid,))
            row = cur.fetchone()
            if not row: raise HTTPException(status_code=404, detail="profile not found")
            merged = {"domains": normalize_list((row["domains"] or []) + extracted["domains"]), "roles": normalize_list((row["roles"] or []) + extracted["roles"]), "experience_years": max(int(row["experience_years"]), int(extracted["experience_years"] or 0)), "networks": normalize_list((row["networks"] or []) + extracted["networks"]), "assets": normalize_list((row["assets"] or []) + extracted["assets"]), "values": normalize_list((row["values"] or []) + extracted["values"]), "attributes": {**(row["attributes"] or {}), **(extracted["attributes"] or {})}}
            cur.execute("UPDATE member_profiles SET domains_json=%s, roles_json=%s, experience_years=%s, networks_json=%s, assets_json=%s, values_json=%s, attributes_json=%s, strength_score=%s, updated_at=NOW() WHERE member_id=%s", (Json(merged["domains"]), Json(merged["roles"]), merged["experience_years"], Json(merged["networks"]), Json(merged["assets"]), Json(merged["values"]), Json(merged["attributes"]), profile_strength_score(merged), row["member_id"]))
            cur.execute("INSERT INTO member_documents (member_id, filename, content_type, extracted_text, parsed_json) VALUES (%s,%s,%s,%s,%s)", (row["member_id"], file.filename or "upload", file.content_type, text[:100000], Json(extracted)))
        return JSONResponse({"ok": True, "message": "Document parsed and profile updated.", "extracted": extracted})
    finally:
        put_conn(conn)
@app.get("/api/rankings")
def api_rankings(limit: int = 60):
    profiles = fetch_profiles(5000); conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            out = []
            for p in profiles:
                cur.execute("SELECT COUNT(*) AS c FROM pings WHERE status='accepted' AND (requester_gmid=%s OR target_gmid=%s)", (p["gmid"], p["gmid"])); connections = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM pings WHERE requester_gmid=%s", (p["gmid"],)); sent = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM pings WHERE requester_gmid=%s AND status='accepted'", (p["gmid"],)); accepted_sent = cur.fetchone()["c"]
                response_rate = (accepted_sent / sent) if sent else 0; strength = profile_strength_score(p); composite = round((strength * 0.72) + (min(connections, 10) * 2.0) + (response_rate * 8.0), 2)
                out.append({"gmid": p["gmid"], "display_name": p["display_name"], "strength_score": strength, "connections": connections, "response_rate": round(response_rate, 2), "composite_score": composite, "domains": p["domains"]})
            out.sort(key=lambda x: (x["composite_score"], x["strength_score"], x["connections"]), reverse=True)
            for idx, row in enumerate(out, start=1): row["rank"] = idx
            return JSONResponse({"ok": True, "items": out[:limit]})
    finally:
        put_conn(conn)
@app.post("/api/match")
async def api_match(request: Request):
    data = await request.json(); q = (data.get("query") or "").strip(); requester = (data.get("requester_gmid") or "").strip()
    if not q: raise HTTPException(status_code=400, detail="query is required")
    profiles = [p for p in fetch_profiles(5000) if p["gmid"] != requester]
    scored = [(score_profile(q, p), p) for p in profiles]; scored = [(s, p) for s, p in scored if s > 0]; scored.sort(key=lambda x: (x[0], profile_strength_score(x[1])), reverse=True)
    out = []
    for s, p in scored[:10]: out.append({"score": int(s), "profile": {"gmid": p["gmid"], "display_name": p["display_name"], "domains": p["domains"], "roles": p["roles"], "experience_years": p["experience_years"], "assets_preview": (p["assets"] or [])[:6], "networks": p["networks"], "is_system": p["is_system"], "strength_score": profile_strength_score(p)}})
    return JSONResponse({"ok": True, "query": q, "count": len(out), "results": out})
@app.post("/api/ping")
async def api_ping(request: Request):
    data = await request.json(); requester = (data.get("requester_gmid") or "").strip(); target = (data.get("target_gmid") or "").strip(); txt = (data.get("request_text") or "").strip(); score = int(data.get("score") or 0)
    if not requester or not target or not txt: raise HTTPException(status_code=400, detail="requester_gmid, target_gmid, request_text required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("INSERT INTO pings (requester_gmid, target_gmid, request_text, score, status) VALUES (%s,%s,%s,%s,'pending') RETURNING id", (requester, target, txt, score)); ping_id = cur.fetchone()["id"]
        return JSONResponse({"ok": True, "ping_id": ping_id})
    finally:
        put_conn(conn)
@app.get("/api/inbox/{gmid}")
def api_inbox(gmid: str, limit: int = 200):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT p.*, req.display_name AS requester_name, tgt.display_name AS target_name FROM pings p LEFT JOIN members req ON req.gmid = p.requester_gmid LEFT JOIN members tgt ON tgt.gmid = p.target_gmid WHERE p.target_gmid=%s ORDER BY p.created_at DESC LIMIT %s", (gmid, limit))
            return JSONResponse({"ok": True, "items": cur.fetchall()})
    finally:
        put_conn(conn)
@app.get("/api/outbox/{gmid}")
def api_outbox(gmid: str, limit: int = 200):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT p.*, req.display_name AS requester_name, tgt.display_name AS target_name FROM pings p LEFT JOIN members req ON req.gmid = p.requester_gmid LEFT JOIN members tgt ON tgt.gmid = p.target_gmid WHERE p.requester_gmid=%s ORDER BY p.created_at DESC LIMIT %s", (gmid, limit))
            return JSONResponse({"ok": True, "items": cur.fetchall()})
    finally:
        put_conn(conn)
@app.post("/api/ping/{ping_id}/respond")
async def api_respond(ping_id: int, request: Request):
    data = await request.json(); status = (data.get("status") or "").strip().lower()
    if status not in ("accepted", "declined"): raise HTTPException(status_code=400, detail="status must be accepted or declined")
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur: cur.execute("UPDATE pings SET status=%s, responded_at=NOW() WHERE id=%s", (status, ping_id))
        return JSONResponse({"ok": True, "ping_id": ping_id, "status": status})
    finally:
        put_conn(conn)
@app.get("/api/chat/{ping_id}")
def api_chat(ping_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, requester_gmid, target_gmid, status FROM pings WHERE id=%s", (ping_id,)); ping = cur.fetchone()
            if not ping: raise HTTPException(status_code=404, detail="ping not found")
            if ping["status"] != "accepted": raise HTTPException(status_code=400, detail="chat available only after accept")
            cur.execute("SELECT * FROM chat_messages WHERE ping_id=%s ORDER BY id ASC", (ping_id,))
            return JSONResponse({"ok": True, "ping": ping, "messages": cur.fetchall()})
    finally:
        put_conn(conn)
@app.post("/api/chat/{ping_id}/send")
async def api_chat_send(ping_id: int, request: Request):
    data = await request.json(); sender = (data.get("sender_gmid") or "").strip(); msg = (data.get("message") or "").strip()
    if not sender or not msg: raise HTTPException(status_code=400, detail="sender_gmid and message required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT status FROM pings WHERE id=%s", (ping_id,)); ping = cur.fetchone()
            if not ping: raise HTTPException(status_code=404, detail="ping not found")
            if ping["status"] != "accepted": raise HTTPException(status_code=400, detail="chat available only after accept")
            cur.execute("INSERT INTO chat_messages (ping_id, sender_gmid, message) VALUES (%s,%s,%s)", (ping_id, sender, msg))
        return JSONResponse({"ok": True})
    finally:
        put_conn(conn)
@app.get("/api/network/{gmid}")
def api_network(gmid: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT p.*, req.display_name AS requester_name, tgt.display_name AS target_name FROM pings p LEFT JOIN members req ON req.gmid = p.requester_gmid LEFT JOIN members tgt ON tgt.gmid = p.target_gmid WHERE p.status='accepted' AND (p.requester_gmid=%s OR p.target_gmid=%s) ORDER BY COALESCE(p.responded_at, p.created_at) DESC, p.id DESC", (gmid, gmid))
            rows = cur.fetchall(); nodes, edges, seen = [], [], set()
            def add_node(node_gmid, label):
                if node_gmid not in seen: seen.add(node_gmid); nodes.append({"gmid": node_gmid, "label": label})
            add_node(gmid, "Me")
            for r in rows:
                add_node(r["requester_gmid"], r["requester_name"] or r["requester_gmid"]); add_node(r["target_gmid"], r["target_name"] or r["target_gmid"])
                other = r["target_gmid"] if r["requester_gmid"] == gmid else r["requester_gmid"]; other_name = r["target_name"] if r["requester_gmid"] == gmid else r["requester_name"]
                edges.append({"ping_id": r["id"], "other_gmid": other, "other_name": other_name, "created_at": r["created_at"], "responded_at": r["responded_at"]})
            return JSONResponse({"ok": True, "center_gmid": gmid, "nodes": nodes, "edges": edges})
    finally:
        put_conn(conn)
@app.post("/api/invitations/create")
async def api_create_invitation(request: Request):
    data = await request.json(); candidate_name = (data.get("candidate_name") or "").strip(); candidate_email = (data.get("candidate_email") or "").strip().lower(); reference_gmid = (data.get("reference_gmid") or "").strip(); invited_by_gmid = (data.get("invited_by_gmid") or "").strip(); invite_note = (data.get("invite_note") or "").strip()
    if not candidate_name or not candidate_email or not reference_gmid or not invited_by_gmid: raise HTTPException(status_code=400, detail="candidate_name, candidate_email, reference_gmid, invited_by_gmid required")
    token = secrets.token_urlsafe(24); expires_at = datetime.now(timezone.utc) + timedelta(days=14)
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT gmid FROM members WHERE gmid=%s AND status='active'", (reference_gmid,))
            if not cur.fetchone(): raise HTTPException(status_code=400, detail="Reference GMID is not an active community member.")
            cur.execute("INSERT INTO member_invitations (candidate_name, candidate_email, reference_gmid, invited_by_gmid, invitation_token, invite_note, expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id", (candidate_name, candidate_email, reference_gmid, invited_by_gmid, token, invite_note, expires_at)); invitation_id = cur.fetchone()["id"]
            cur.execute("INSERT INTO member_references (invitation_id, reference_gmid, sponsor_gmid) VALUES (%s,%s,%s)", (invitation_id, reference_gmid, invited_by_gmid))
        return JSONResponse({"ok": True, "invitation_id": invitation_id, "token": token, "invite_link": f"{APP_BASE_URL}/invite/{token}", "email_subject": "Congratulations — You’ve Been Invited to Join Meridian", "email_body": f"Congratulations — You’ve been invited to join Meridian. This invitation was sponsored by a member of the community. Complete your vetted profile here: {APP_BASE_URL}/invite/{token}"})
    finally:
        put_conn(conn)
@app.get("/api/invitations/{token}")
def api_get_invitation(token: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM member_invitations WHERE invitation_token=%s", (token,)); row = cur.fetchone()
            if not row: raise HTTPException(status_code=404, detail="Invitation not found")
            return JSONResponse({"ok": True, "invitation": row})
    finally:
        put_conn(conn)
@app.post("/api/invitations/{token}/complete")
async def api_complete_invitation(token: str, request: Request):
    data = await request.json(); display_name = (data.get("display_name") or "").strip(); email = (data.get("email") or "").strip().lower()
    if not display_name or not email: raise HTTPException(status_code=400, detail="display_name and email required")
    domains = normalize_list(data.get("domains")); roles = normalize_list(data.get("roles")); networks = normalize_list(data.get("networks")); values = normalize_list(data.get("values")); assets = normalize_list(data.get("assets")); experience_years = int(data.get("experience_years") or 0); attributes = data.get("attributes") or {}
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM member_invitations WHERE invitation_token=%s AND invitation_status='sent'", (token,)); inv = cur.fetchone()
            if not inv: raise HTTPException(status_code=404, detail="Invitation not found or already completed")
            gmid = make_gmid(display_name + "|" + email)
            cur.execute("INSERT INTO members (gmid, display_name, email, is_system, status) VALUES (%s,%s,%s,FALSE,'pending_vetting') RETURNING id", (gmid, display_name, email)); member_id = cur.fetchone()["id"]
            profile = {"domains": domains, "roles": roles, "experience_years": experience_years, "networks": networks, "assets": assets, "values": values, "attributes": attributes}
            cur.execute("INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (member_id, Json(domains), Json(roles), experience_years, Json(networks), Json(assets), Json(values), Json(attributes), profile_strength_score(profile)))
            cur.execute("UPDATE member_invitations SET invitation_status='accepted', accepted_at=NOW() WHERE id=%s", (inv["id"],))
        return JSONResponse({"ok": True, "gmid": gmid, "status": "pending_vetting"})
    finally:
        put_conn(conn)
@app.get("/api/debug/db")
def api_debug():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM members"); members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM pings"); pings = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM chat_messages"); chats = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM member_invitations"); invites = cur.fetchone()["c"]
            return JSONResponse({"database": "postgres", "members": members, "pings": pings, "chat_messages": chats, "invitations": invites})
    finally:
        put_conn(conn)
