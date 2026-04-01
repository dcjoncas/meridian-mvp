
import os, json, hashlib, random, datetime, sqlite3, re, io, html
from typing import List, Dict, Any
from fastapi import FastAPI, Request, UploadFile, File
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
    return [t.lower() for t in re.findall(r"[a-zA-Z0-9&\-/]+", text or "") if len(t) > 1]

def normalize_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        vals = value
    else:
        vals = [x.strip() for x in str(value).split(",")]
    out, seen = [], set()
    for v in vals:
        s = str(v).strip()
        k = s.lower()
        if s and k not in seen:
            out.append(s)
            seen.add(k)
    return out

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

DOMAIN_KEYWORDS = [
    "Manufacturing","Aerospace","Defense","Semiconductors","Automotive",
    "Industrial Automation","Supply Chain","Logistics","Procurement",
    "Quality Systems","Lean / Six Sigma","Plant Ops","Construction",
    "Energy","Renewables","Utilities","Oil & Gas","Healthcare",
    "Financial Services","Private Equity","Venture Capital","Cybersecurity",
    "Cloud Infrastructure","Data Platforms","AI/ML","ERP Transformations",
    "Program Delivery","Compliance","Risk & Controls","Executive Search",
    "Strategic Introductions"
]

ROLE_KEYWORDS = [
    "COO","CTO","VP Manufacturing","Plant Director","Director of Operations",
    "Head of Supply Chain","Head of Procurement","Quality Director",
    "Transformation Lead","Operating Advisor","Program Executive","CISO",
    "Data Platform Lead","Integration Architect","Finance Transformation Lead",
    "Principal","Managing Partner"
]

NETWORK_KEYWORDS = [
    "Global LP network","C-suite operator channel","Board-level advisory",
    "Fortune 500 operator network","Global tier-1 suppliers","PE operating partners",
    "OEM executive channel","Cloud provider exec channel","Defense-industrial base partners",
    "Board network","Executive search network","Investor network"
]

VALUE_KEYWORDS = [
    "Discretion","Reciprocity","Outcome rigor","Trust","Non-attribution",
    "Calm under pressure","Zero-ego execution","Signal over noise"
]

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
        non_empty = 0
        for v in attrs.values():
            if isinstance(v, list) and v:
                non_empty += 1
            elif v not in (None, "", [], {}):
                non_empty += 1
        score += min(8, non_empty)
    return min(100, score)

def extract_years_from_query(query: str):
    m = re.search(r"(\d+)\+?\s+years?", query.lower())
    if m:
        return int(m.group(1))
    return None

def weighted_hits(query: str, items: List[str], exact_weight: int, token_weight: int) -> int:
    q = query.lower()
    q_tokens = set(tokenize(query))
    score = 0
    for item in items:
        item_l = str(item).lower()
        if item_l in q or q in item_l:
            score += exact_weight
        else:
            item_tokens = set(tokenize(item_l))
            overlap = len(item_tokens & q_tokens)
            score += overlap * token_weight
    return score

def score_profile(query: str, p: Dict[str, Any]) -> int:
    if not query.strip():
        return 0
    q = query.lower()
    score = 0
    score += weighted_hits(query, p.get("domains", []), 18, 6)
    score += weighted_hits(query, p.get("roles", []), 16, 5)
    score += weighted_hits(query, p.get("networks", []), 9, 3)
    score += weighted_hits(query, p.get("values", []), 7, 2)
    score += weighted_hits(query, (p.get("assets") or [])[:20], 4, 1)

    desired_years = extract_years_from_query(query)
    exp = int(p.get("experience_years") or 0)
    if desired_years is not None:
        gap = abs(exp - desired_years)
        score += max(0, 14 - min(14, gap))
    else:
        score += min(8, max(0, exp // 2))

    strength = profile_strength_score(p)
    score += strength * 0.12

    # targeted elite bonuses
    elite_map = {
        "private equity": ["Private Equity", "Operating Advisor", "Principal", "Managing Partner"],
        "manufacturing": ["Manufacturing", "VP Manufacturing", "Plant Director", "Plant Ops"],
        "supply chain": ["Supply Chain", "Head of Supply Chain", "Procurement", "Logistics"],
        "cybersecurity": ["Cybersecurity", "CISO"],
        "finance": ["Financial Services", "Finance Transformation Lead"],
        "executive search": ["Executive Search", "Strategic Introductions"]
    }
    for phrase, concepts in elite_map.items():
        if phrase in q:
            bag = " | ".join([*p.get("domains", []), *p.get("roles", []), *p.get("networks", [])]).lower()
            for concept in concepts:
                if concept.lower() in bag:
                    score += 6

    # deterministic tie-breaker so scores are rarely identical
    tie = int(hashlib.sha256((query + "|" + p["gmid"]).encode("utf-8")).hexdigest()[:2], 16) / 255.0
    score += tie * 1.9

    return max(0, min(100, int(round(score))))

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

        first = ["Avery","Jordan","Riley","Casey","Morgan","Taylor","Quinn","Hayden","Parker","Rowan","Blake","Cameron","Drew","Emerson","Finley","Harper","Kai","Logan","Micah","Noel"]
        last  = ["Stone","Reed","Carter","Hayes","Brooks","Wells","Foster","Shaw","Bennett","Cole","Sullivan","Pierce","Vaughn","Donovan","Holland","Walsh","Hayward","Monroe","Kendall","Navarro"]

        for i in range(target_count):
            rnd = random.Random(i + 77)
            display = f"{first[i % len(first)]} {last[(i*3) % len(last)]} — EX-{i+1:03d}"
            gmid = make_gmid("SYSTEM|" + display)
            domains = rnd.sample(DOMAIN_KEYWORDS[:24], k=rnd.randint(2, 4))
            roles = rnd.sample(ROLE_KEYWORDS[:15], k=rnd.randint(1, 2))
            networks = rnd.sample(NETWORK_KEYWORDS, k=rnd.randint(1, 3))
            assets = rnd.sample([
                "Ran a multi-site manufacturing turnaround","Commissioned a greenfield plant",
                "Reduced scrap by double-digits","Improved OEE by 10+ points",
                "Implemented a tiered daily management system","Built a best-in-class maintenance program",
                "Established a robust supplier quality program","Led a major supplier renegotiation",
                "Built S&OP cadence and governance","Reduced inventory without hurting service",
                "Delivered ERP fit-to-standard with minimal custom","Cut over a complex ERP deployment",
                "Led CPI/integration modernization","Implemented zero-trust segmentation",
                "Executed a carve-out / TSA separation","Supported a post-merger integration",
                "Executed a cost takeout program","Negotiated strategic long-term supply agreements",
                "Improved safety performance","Improved first-pass yield","Introduced standard work and coaching",
                "Executed international expansion","Built an investor-grade operating rhythm"
            ], k=rnd.randint(8, 14))
            values = rnd.sample(VALUE_KEYWORDS, k=rnd.randint(2, 4))
            attrs = {
                "engagement_type": rnd.choice(["advisory retainer","short sprint","transformation lead"]),
                "availability": rnd.choice(["near-term","limited","immediate"]),
                "confidentiality": "non-attribution",
                "notes": "SYSTEM profile"
            }
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
            try:
                from PyPDF2 import PdfReader
                reader = PdfReader(io.BytesIO(raw))
                return "\n".join((page.extract_text() or "") for page in reader.pages)
            except Exception:
                return ""
    return raw.decode("utf-8", errors="ignore")

def parse_profile_text(text: str) -> Dict[str, Any]:
    text = text or ""
    lower = text.lower()
    domains = [d for d in DOMAIN_KEYWORDS if d.lower() in lower]
    roles = [r for r in ROLE_KEYWORDS if r.lower() in lower]
    networks = [n for n in NETWORK_KEYWORDS if n.lower() in lower]
    values = [v for v in VALUE_KEYWORDS if v.lower() in lower]

    years = 0
    years_matches = re.findall(r"(\d+)\+?\s+years?", lower)
    if years_matches:
        years = max(int(x) for x in years_matches)

    lines = [re.sub(r"\s+", " ", x).strip(" -•\t") for x in re.split(r"[\n\r]+|[;]", text)]
    assets = []
    for line in lines:
        if len(line) < 18:
            continue
        if any(word in line.lower() for word in ["led","built","delivered","improved","implemented","executed","created","managed","launched","reduced","increased","designed","negotiated","supported","stabilized"]):
            if line not in assets:
                assets.append(line)
        if len(assets) >= 12:
            break

    if not assets:
        sentences = [x.strip() for x in re.split(r"[.!?]", text) if len(x.strip()) > 20]
        assets = sentences[:8]

    attributes = {}
    if "board" in lower:
        attributes["board_experience"] = True
    if "sap" in lower:
        attributes["sap_experience"] = True
    if "oracle" in lower:
        attributes["oracle_experience"] = True
    if "azure" in lower:
        attributes["azure_experience"] = True
    if "aws" in lower:
        attributes["aws_experience"] = True
    if "global" in lower:
        attributes["global_scope"] = True
    if "confidential" in lower or "discreet" in lower or "discreet" in lower:
        attributes["confidentiality"] = "high"

    return {
        "domains": domains,
        "roles": roles,
        "networks": networks,
        "values": values,
        "assets": assets,
        "experience_years": years,
        "attributes": attributes,
        "extracted_preview": text[:1200]
    }

app = FastAPI(title="Meridian MVP", version="16.0")
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

@app.get("/rankings", response_class=HTMLResponse)
def rankings_page():
    return HTMLResponse(open(os.path.join(BASE_DIR, "rankings.html"), "r", encoding="utf-8").read())

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

@app.get("/api/profile_summary/{gmid}")
def api_profile_summary(gmid: str):
    con = connect()
    try:
        rows = con.execute("SELECT * FROM profiles").fetchall()
        profs = [row_to_profile(r) for r in rows]
        current = next((p for p in profs if p["gmid"] == gmid), None)
        if not current:
            return JSONResponse({"ok": False, "error": "profile not found"}, status_code=404)
        scored = sorted(
            [{"gmid": p["gmid"], "score": profile_strength_score(p)} for p in profs],
            key=lambda x: x["score"],
            reverse=True
        )
        rank = next((i + 1 for i, row in enumerate(scored) if row["gmid"] == gmid), None)
        total_connections = con.execute(
            "SELECT COUNT(*) AS c FROM pings WHERE status='accepted' AND (requester_gmid=? OR target_gmid=?)",
            (gmid, gmid)
        ).fetchone()["c"]
        return JSONResponse({
            "ok": True,
            "rank": rank,
            "total_profiles": len(scored),
            "strength_score": profile_strength_score(current),
            "total_connections": total_connections,
            "profile": current
        })
    finally:
        con.close()

@app.post("/api/profile/{gmid}/update")
async def api_profile_update(gmid: str, request: Request):
    data = await request.json()
    con = connect()
    try:
        row = con.execute("SELECT * FROM profiles WHERE gmid=?", (gmid,)).fetchone()
        if not row:
            return JSONResponse({"ok": False, "error": "profile not found"}, status_code=404)
        current = row_to_profile(row)
        display_name = (data.get("display_name") or current["display_name"]).strip()
        experience_years = int(data.get("experience_years") if data.get("experience_years") not in (None, "") else current["experience_years"])
        domains = normalize_list(data.get("domains")) or current["domains"]
        roles = normalize_list(data.get("roles")) or current["roles"]
        networks = normalize_list(data.get("networks")) or current["networks"]
        assets = normalize_list(data.get("assets")) or current["assets"]
        values = normalize_list(data.get("values")) or current["values"]
        attributes = current["attributes"] or {}
        new_attrs = data.get("attributes") or {}
        if isinstance(new_attrs, dict):
            for k, v in new_attrs.items():
                if v not in (None, "", [], {}):
                    attributes[k] = v

        con.execute(
            """UPDATE profiles SET
                display_name=?,
                domains_json=?,
                roles_json=?,
                experience_years=?,
                networks_json=?,
                assets_json=?,
                values_json=?,
                attributes_json=?
               WHERE gmid=?""",
            (
                display_name,
                dumps(domains),
                dumps(roles),
                experience_years,
                dumps(networks),
                dumps(assets),
                dumps(values),
                dumps(attributes),
                gmid,
            )
        )
        con.commit()
        return JSONResponse({"ok": True})
    finally:
        con.close()

@app.post("/api/profile/{gmid}/ingest-document")
async def api_profile_ingest(gmid: str, file: UploadFile = File(...)):
    raw = await file.read()
    text = extract_text_from_upload(file, raw)
    if not text.strip():
        return JSONResponse({"ok": False, "error": "Could not extract readable text from the uploaded document."}, status_code=400)

    extracted = parse_profile_text(text)

    con = connect()
    try:
        row = con.execute("SELECT * FROM profiles WHERE gmid=?", (gmid,)).fetchone()
        if not row:
            return JSONResponse({"ok": False, "error": "profile not found"}, status_code=404)
        current = row_to_profile(row)

        merged = {
            "domains": normalize_list(current["domains"] + extracted["domains"]),
            "roles": normalize_list(current["roles"] + extracted["roles"]),
            "networks": normalize_list(current["networks"] + extracted["networks"]),
            "values": normalize_list(current["values"] + extracted["values"]),
            "assets": normalize_list(current["assets"] + extracted["assets"]),
            "experience_years": max(int(current["experience_years"]), int(extracted["experience_years"] or 0)),
            "attributes": {**(current["attributes"] or {}), **(extracted["attributes"] or {})}
        }

        con.execute(
            """UPDATE profiles SET
                domains_json=?,
                roles_json=?,
                experience_years=?,
                networks_json=?,
                assets_json=?,
                values_json=?,
                attributes_json=?
               WHERE gmid=?""",
            (
                dumps(merged["domains"]),
                dumps(merged["roles"]),
                merged["experience_years"],
                dumps(merged["networks"]),
                dumps(merged["assets"]),
                dumps(merged["values"]),
                dumps(merged["attributes"]),
                gmid
            )
        )
        con.commit()
        return JSONResponse({
            "ok": True,
            "message": "Document parsed and profile updated.",
            "extracted": extracted,
            "merged": merged
        })
    finally:
        con.close()

@app.get("/api/rankings")
def api_rankings(limit: int = 50):
    con = connect()
    try:
        rows = con.execute("SELECT * FROM profiles").fetchall()
        profs = [row_to_profile(r) for r in rows]
        out = []
        for p in profs:
            connections = con.execute(
                "SELECT COUNT(*) AS c FROM pings WHERE status='accepted' AND (requester_gmid=? OR target_gmid=?)",
                (p["gmid"], p["gmid"])
            ).fetchone()["c"]
            sent = con.execute("SELECT COUNT(*) AS c FROM pings WHERE requester_gmid=?", (p["gmid"],)).fetchone()["c"]
            accepted_sent = con.execute("SELECT COUNT(*) AS c FROM pings WHERE requester_gmid=? AND status='accepted'", (p["gmid"],)).fetchone()["c"]
            response_rate = (accepted_sent / sent) if sent else 0
            strength = profile_strength_score(p)
            composite = round((strength * 0.72) + (min(connections, 10) * 2.0) + (response_rate * 8.0), 2)
            out.append({
                "gmid": p["gmid"],
                "display_name": p["display_name"],
                "is_system": p["is_system"],
                "strength_score": strength,
                "connections": connections,
                "response_rate": round(response_rate, 2),
                "composite_score": composite,
                "domains": p["domains"],
                "roles": p["roles"],
            })
        out.sort(key=lambda x: (x["composite_score"], x["strength_score"], x["connections"]), reverse=True)
        for idx, row in enumerate(out, start=1):
            row["rank"] = idx
        return JSONResponse({"ok": True, "items": out[:limit]})
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
        scored.sort(key=lambda x: (x[0], profile_strength_score(x[1])), reverse=True)
        out = []
        for s, p in scored[:10]:
            out.append({
                "score": int(s),
                "profile": {
                    "gmid": p["gmid"],
                    "display_name": p["display_name"],
                    "domains": p["domains"],
                    "roles": p["roles"],
                    "experience_years": p["experience_years"],
                    "assets_preview": (p["assets"] or [])[:6],
                    "networks": p["networks"],
                    "is_system": p["is_system"],
                    "strength_score": profile_strength_score(p)
                }
            })
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


@app.get("/api/network/{gmid}")
def api_network(gmid: str):
    con = connect()
    try:
        rows = con.execute(
            """SELECT p.*, pr.display_name AS requester_name, pt.display_name AS target_name
               FROM pings p
               LEFT JOIN profiles pr ON pr.gmid = p.requester_gmid
               LEFT JOIN profiles pt ON pt.gmid = p.target_gmid
               WHERE p.status='accepted' AND (p.requester_gmid=? OR p.target_gmid=?)
               ORDER BY COALESCE(p.responded_at, p.created_at) DESC, p.id DESC""",
            (gmid, gmid)
        ).fetchall()
        nodes = []
        edges = []
        seen_nodes = set()
        def add_node(node_gmid, label):
            if node_gmid not in seen_nodes:
                seen_nodes.add(node_gmid)
                nodes.append({"gmid": node_gmid, "label": label})
        add_node(gmid, "Me")
        for r in rows:
            requester = r["requester_gmid"]
            target = r["target_gmid"]
            requester_name = r["requester_name"] or requester
            target_name = r["target_name"] or target
            add_node(requester, requester_name)
            add_node(target, target_name)
            other = target if requester == gmid else requester
            other_name = target_name if requester == gmid else requester_name
            edges.append({
                "ping_id": r["id"],
                "from_gmid": requester,
                "to_gmid": target,
                "other_gmid": other,
                "other_name": other_name,
                "created_at": r["created_at"],
                "responded_at": r["responded_at"],
            })
        return JSONResponse({"ok": True, "center_gmid": gmid, "nodes": nodes, "edges": edges})
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
