
import os, json, hashlib, random, datetime, sqlite3
from typing import List, Dict, Any
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.getenv("MERIDIAN_DB") or os.path.join(BASE_DIR, "meridian_v13.db")

def utcnow_iso() -> str:
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def dumps(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False)

def loads(s: str, default):
    try:
        return json.loads(s)
    except Exception:
        return default

def make_gmid(seed: str) -> str:
    return hashlib.sha256((seed + "|MERIDIAN_V13").encode("utf-8")).hexdigest()

def tokenize(text: str) -> List[str]:
    import re
    return [t.lower() for t in re.findall(r"[a-zA-Z0-9&\-/]+", text or "") if len(t) > 1]

def row_to_profile(r: sqlite3.Row) -> Dict[str, Any]:
    return {
        "gmid":             r["gmid"],
        "is_system":        int(r["is_system"]) if ("is_system" in r.keys()) else 0,
        "display_name":     r["display_name"],
        "domains":          loads(r["domains_json"], []),
        "roles":            loads(r["roles_json"], []),
        "experience_years": int(r["experience_years"]),
        "networks":         loads(r["networks_json"], []),
        "political_social": loads(r["political_social_json"], []),
        "assets":           loads(r["assets_json"], []),
        "values":           loads(r["values_json"], []),
        "attributes":       loads(r["attributes_json"], {}),
        "created_at":       r["created_at"],
    }

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
  gmid                TEXT PRIMARY KEY,
  is_system           INTEGER NOT NULL DEFAULT 0,
  display_name        TEXT NOT NULL,
  domains_json        TEXT NOT NULL,
  roles_json          TEXT NOT NULL,
  experience_years    INTEGER NOT NULL,
  networks_json       TEXT NOT NULL,
  political_social_json TEXT NOT NULL,
  assets_json         TEXT NOT NULL,
  values_json         TEXT NOT NULL,
  attributes_json     TEXT NOT NULL,
  created_at          TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pings (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  requester_gmid  TEXT NOT NULL,
  target_gmid     TEXT NOT NULL,
  request_text    TEXT NOT NULL,
  score           INTEGER NOT NULL,
  status          TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  responded_at    TEXT
);

CREATE TABLE IF NOT EXISTS chat_messages (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  ping_id      INTEGER NOT NULL,
  sender_gmid  TEXT NOT NULL,
  message      TEXT NOT NULL,
  created_at   TEXT NOT NULL
);
"""

def init_db() -> None:
    con = connect()
    try:
        con.executescript(SCHEMA_SQL)
        try:
            con.execute("ALTER TABLE profiles ADD COLUMN is_system INTEGER NOT NULL DEFAULT 0")
        except Exception:
            pass
        con.commit()
    finally:
        con.close()

def completeness_ok(p: Dict[str, Any]) -> bool:
    return (
        bool(p.get("display_name"))
        and bool(p.get("domains"))
        and bool(p.get("roles"))
        and int(p.get("experience_years") or 0) >= 1
        and isinstance(p.get("assets"), list)
        and len(p.get("assets") or []) >= 5
    )

def score_profile(query: str, p: Dict[str, Any]) -> int:
    q_tokens = set(tokenize(query))
    if not q_tokens:
        return 0
    fields: List[str] = []
    fields += [str(x) for x in p.get("domains", [])]
    fields += [str(x) for x in p.get("roles", [])]
    fields += [str(x) for x in p.get("networks", [])]
    fields += [str(x) for x in p.get("political_social", [])]
    fields += [str(x) for x in p.get("values", [])]
    fields += [str(x) for x in (p.get("assets") or [])[:30]]
    attrs = p.get("attributes") or {}
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            fields.append(str(k))
            if isinstance(v, (list, tuple)):
                fields += [str(x) for x in v[:10]]
            else:
                fields.append(str(v))
    field_tokens = set(tokenize(" ".join(fields)))
    overlap = len(q_tokens & field_tokens)
    base = min(70, overlap * 7)
    exp_pts = min(12, max(0, int(p.get("experience_years") or 0) - 5))
    score = max(0, min(100, base + exp_pts))
    if overlap == 0:
        return 0
    return score

def seed_mike_s() -> None:
    con = connect()
    try:
        exists = con.execute(
            "SELECT gmid FROM profiles WHERE display_name=? AND is_system=0",
            ("Mike S",)
        ).fetchone()
        if exists:
            return
        gmid = make_gmid("Mike S|PRINCIPAL")
        con.execute(
            """INSERT OR IGNORE INTO profiles
               (gmid, is_system, display_name, domains_json, roles_json,
                experience_years, networks_json, political_social_json,
                assets_json, values_json, attributes_json, created_at)
               VALUES (?,0,?,?,?,?,?,?,?,?,?,?)""",
            (
                gmid,
                "Mike S",
                dumps(["Private Equity", "Financial Services", "Executive Search", "Strategic Introductions"]),
                dumps(["Principal", "Managing Partner"]),
                18,
                dumps(["Global LP network", "C-suite operator channel", "Board-level advisory"]),
                dumps(["Discretion-first collaboration", "Non-attribution protocol"]),
                dumps([
                    "20+ years executing discreet executive mandates",
                    "Deep LP and sovereign fund relationships",
                    "Multi-sector board and operating network",
                    "Cross-border deal origination track record",
                    "Non-attributable introduction protocol",
                    "Known for zero-ego, outcome-first execution",
                ]),
                dumps(["Discretion", "Reciprocity", "Outcome rigor"]),
                dumps({"engagement_type":"advisory","confidentiality":"non-attribution","notes":"Principal account — Mike S. Login: mike / red123"}),
                utcnow_iso(),
            )
        )
        con.commit()
    finally:
        con.close()

def seed_system_profiles_once(target_count: int = 100) -> None:
    con = connect()
    try:
        sys_count = con.execute("SELECT COUNT(*) AS c FROM profiles WHERE is_system=1").fetchone()["c"]
        if sys_count > 0:
            return
        domains_pool = ["Manufacturing","Aerospace","Defense","Semiconductors","Automotive","Industrial Automation","Supply Chain","Logistics","Procurement","Quality Systems","Lean / Six Sigma","Plant Ops","Construction","Energy","Healthcare","Financial Services","Private Equity","Cybersecurity","Cloud Infrastructure","Data Platforms","AI/ML","ERP Transformations","Program Delivery","Compliance"]
        roles_pool = ["COO","CTO","VP Manufacturing","Plant Director","Director of Operations","Head of Supply Chain","Head of Procurement","Quality Director","Transformation Lead","Operating Advisor","Program Executive","CISO","Data Platform Lead","Integration Architect","Finance Transformation Lead"]
        networks_pool = ["Fortune 500 operator network","Global tier-1 suppliers","PE operating partners","OEM executive channel","Cloud provider exec channel","Defense-industrial base partners"]
        values_pool = ["Discretion-first collaboration","Outcome rigor","Signal over noise","Trust and reciprocity","Non-attribution","Calm under pressure","Zero-ego execution"]
        asset_pool = ["Ran a multi-site manufacturing turnaround","Commissioned a greenfield plant","Reduced scrap by double-digits","Improved OEE by 10+ points","Implemented a tiered daily management system","Built a best-in-class maintenance program","Established a robust supplier quality program","Led a major supplier renegotiation","Built S&OP cadence and governance","Reduced inventory without hurting service","Delivered ERP fit-to-standard with minimal custom","Cut over a complex ERP deployment","Led CPI/integration modernization","Implemented zero-trust segmentation","Executed a carve-out / TSA separation","Supported a post-merger integration","Executed a cost takeout program","Negotiated strategic long-term supply agreements","Improved safety performance","Improved first-pass yield","Introduced standard work and coaching","Executed international expansion"]
        first = ["Avery","Jordan","Riley","Casey","Morgan","Taylor","Quinn","Hayden","Parker","Rowan","Blake","Cameron","Drew","Emerson","Finley","Harper","Kai","Logan","Micah","Noel"]
        last  = ["Stone","Reed","Carter","Hayes","Brooks","Wells","Foster","Shaw","Bennett","Cole","Sullivan","Pierce","Vaughn","Donovan","Holland","Walsh","Hayward","Monroe","Kendall","Navarro"]
        for i in range(target_count):
            rnd = random.Random(i + 77)
            display = f"{first[i % len(first)]} {last[(i*3) % len(last)]} — EX-{i+1:03d}"
            gmid = make_gmid("SYSTEM|" + display)
            domains = rnd.sample(domains_pool, k=rnd.randint(2, 4))
            roles = rnd.sample(roles_pool, k=rnd.randint(1, 2))
            networks = rnd.sample(networks_pool, k=rnd.randint(2, 3))
            assets = rnd.sample(asset_pool, k=min(len(asset_pool), rnd.randint(8, 14)))
            values = rnd.sample(values_pool, k=rnd.randint(2, 4))
            attrs = {"engagement_type":"advisory retainer","availability":"near-term","confidentiality":"non-attribution","notes":"SYSTEM profile"}
            con.execute(
                """INSERT OR IGNORE INTO profiles
                   (gmid, is_system, display_name, domains_json, roles_json,
                    experience_years, networks_json, political_social_json,
                    assets_json, values_json, attributes_json, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    gmid, 1, display, dumps(domains), dumps(roles), rnd.randint(8, 28),
                    dumps(networks), dumps([]), dumps(assets), dumps(values), dumps(attrs), utcnow_iso()
                )
            )
        con.commit()
    finally:
        con.close()

app = FastAPI(title="Meridian MVP", version="14.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)

@app.on_event("startup")
def startup():
    init_db()
    seed_mike_s()
    seed_system_profiles_once(100)

@app.get("/", response_class=HTMLResponse)
def home():
    return HTMLResponse(open(os.path.join(BASE_DIR, "ui.html"), "r", encoding="utf-8").read())

@app.get("/member/{gmid}", response_class=HTMLResponse)
def member(gmid: str):
    return HTMLResponse(open(os.path.join(BASE_DIR, "member.html"), "r", encoding="utf-8").read().replace("{{GMID}}", gmid))

@app.get("/alias", response_class=HTMLResponse)
def alias_manager():
    return HTMLResponse(open(os.path.join(BASE_DIR, "GMID.html"), "r", encoding="utf-8").read())

@app.get("/api/profiles")
def api_profiles(limit: int = 200):
    con = connect()
    try:
        rows = con.execute("SELECT * FROM profiles ORDER BY created_at DESC LIMIT ?", (int(limit),)).fetchall()
        return JSONResponse({"count": len(rows), "profiles": [row_to_profile(r) for r in rows]})
    finally:
        con.close()

@app.post("/api/match")
async def api_match(request: Request):
    data = await request.json()
    q = (data.get("query") or "").strip()
    requester = (data.get("requester_gmid") or "").strip()
    if not q:
        return JSONResponse({"ok": False, "error": "query is required"}, status_code=400)
    con = connect()
    try:
        rows = con.execute("SELECT * FROM profiles").fetchall()
        profs = [row_to_profile(r) for r in rows]
        profs = [p for p in profs if completeness_ok(p) and p["gmid"] != requester]
        scored = [(score_profile(q, p), p) for p in profs]
        scored = [(s, p) for s, p in scored if s > 0]
        scored.sort(key=lambda x: x[0], reverse=True)
        out = []
        for s, p in scored[:10]:
            out.append({"score": int(s), "profile": {
                "gmid": p["gmid"], "display_name": p["display_name"], "domains": p["domains"],
                "roles": p["roles"], "experience_years": p["experience_years"],
                "assets_preview": (p["assets"] or [])[:6], "networks": p["networks"], "is_system": p["is_system"]
            }})
        return JSONResponse({"ok": True, "query": q, "count": len(out), "results": out})
    finally:
        con.close()

@app.post("/api/ping")
async def api_ping(request: Request):
    data = await request.json()
    requester = (data.get("requester_gmid") or "").strip()
    target = (data.get("target_gmid") or "").strip()
    txt = (data.get("request_text") or "").strip()
    score = int(data.get("score") or 0)
    if not requester or not target or not txt:
        return JSONResponse({"ok": False, "error": "requester_gmid, target_gmid, request_text required"}, status_code=400)
    con = connect()
    try:
        con.execute(
            "INSERT INTO pings (requester_gmid,target_gmid,request_text,score,status,created_at,responded_at) VALUES (?,?,?,?,?,?,NULL)",
            (requester, target, txt, score, "pending", utcnow_iso())
        )
        con.commit()
        pid = con.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        return JSONResponse({"ok": True, "ping_id": int(pid)})
    finally:
        con.close()

@app.get("/api/inbox/{gmid}")
def api_inbox(gmid: str, limit: int = 200):
    con = connect()
    try:
        rows = con.execute(
            """SELECT p.*, pr.display_name AS requester_name, pt.display_name AS target_name
               FROM pings p
               LEFT JOIN profiles pr ON pr.gmid = p.requester_gmid
               LEFT JOIN profiles pt ON pt.gmid = p.target_gmid
               WHERE p.target_gmid = ?
               ORDER BY p.created_at DESC LIMIT ?""",
            (gmid, int(limit))
        ).fetchall()
        return JSONResponse({"ok": True, "count": len(rows), "items": [dict(r) for r in rows]})
    finally:
        con.close()

@app.get("/api/outbox/{gmid}")
def api_outbox(gmid: str, limit: int = 200):
    con = connect()
    try:
        rows = con.execute(
            """SELECT p.*, pr.display_name AS requester_name, pt.display_name AS target_name
               FROM pings p
               LEFT JOIN profiles pr ON pr.gmid = p.requester_gmid
               LEFT JOIN profiles pt ON pt.gmid = p.target_gmid
               WHERE p.requester_gmid = ?
               ORDER BY p.created_at DESC LIMIT ?""",
            (gmid, int(limit))
        ).fetchall()
        return JSONResponse({"ok": True, "count": len(rows), "items": [dict(r) for r in rows]})
    finally:
        con.close()

@app.get("/api/threads/{gmid}")
def api_threads(gmid: str, limit: int = 400):
    con = connect()
    try:
        rows = con.execute(
            """SELECT p.*, pr.display_name AS requester_name, pt.display_name AS target_name
               FROM pings p
               LEFT JOIN profiles pr ON pr.gmid = p.requester_gmid
               LEFT JOIN profiles pt ON pt.gmid = p.target_gmid
               WHERE p.requester_gmid = ? OR p.target_gmid = ?
               ORDER BY COALESCE(p.responded_at, p.created_at) DESC, p.id DESC
               LIMIT ?""",
            (gmid, gmid, int(limit))
        ).fetchall()
        return JSONResponse({"ok": True, "count": len(rows), "items": [dict(r) for r in rows]})
    finally:
        con.close()

@app.post("/api/ping/{ping_id}/respond")
async def api_respond(ping_id: int, request: Request):
    data = await request.json()
    status = (data.get("status") or "").strip().lower()
    if status not in ("accepted", "declined"):
        return JSONResponse({"ok": False, "error": "status must be accepted or declined"}, status_code=400)
    con = connect()
    try:
        row = con.execute("SELECT id FROM pings WHERE id=?", (int(ping_id),)).fetchone()
        if not row:
            return JSONResponse({"ok": False, "error": "ping not found"}, status_code=404)
        con.execute("UPDATE pings SET status=?, responded_at=? WHERE id=?", (status, utcnow_iso(), int(ping_id)))
        con.commit()
        return JSONResponse({"ok": True, "ping_id": int(ping_id), "status": status})
    finally:
        con.close()

@app.get("/api/chat/{ping_id}")
def api_chat(ping_id: int):
    con = connect()
    try:
        ping = con.execute("SELECT id, requester_gmid, target_gmid, status FROM pings WHERE id=?", (int(ping_id),)).fetchone()
        if not ping:
            return JSONResponse({"ok": False, "error": "ping not found"}, status_code=404)
        if ping["status"] != "accepted":
            return JSONResponse({"ok": False, "error": "chat available only after accept"}, status_code=400)
        rows = con.execute("SELECT * FROM chat_messages WHERE ping_id=? ORDER BY id ASC", (int(ping_id),)).fetchall()
        return JSONResponse({"ok": True, "ping": dict(ping), "messages": [dict(r) for r in rows]})
    finally:
        con.close()

@app.post("/api/chat/{ping_id}/send")
async def api_chat_send(ping_id: int, request: Request):
    data = await request.json()
    sender = (data.get("sender_gmid") or "").strip()
    msg = (data.get("message") or "").strip()
    if not sender or not msg:
        return JSONResponse({"ok": False, "error": "sender_gmid and message required"}, status_code=400)
    con = connect()
    try:
        ping = con.execute("SELECT status FROM pings WHERE id=?", (int(ping_id),)).fetchone()
        if not ping:
            return JSONResponse({"ok": False, "error": "ping not found"}, status_code=404)
        if ping["status"] != "accepted":
            return JSONResponse({"ok": False, "error": "chat available only after accept"}, status_code=400)
        con.execute("INSERT INTO chat_messages (ping_id,sender_gmid,message,created_at) VALUES (?,?,?,?)", (int(ping_id), sender, msg, utcnow_iso()))
        con.commit()
        return JSONResponse({"ok": True})
    finally:
        con.close()

@app.get("/api/debug/db")
def api_debug():
    con = connect()
    try:
        prof = con.execute("SELECT COUNT(*) c FROM profiles").fetchone()["c"]
        pings = con.execute("SELECT COUNT(*) c FROM pings").fetchone()["c"]
        chats = con.execute("SELECT COUNT(*) c FROM chat_messages").fetchone()["c"]
        return JSONResponse({"db_path": DB_PATH, "profiles": prof, "pings": pings, "chat_messages": chats})
    finally:
        con.close()
