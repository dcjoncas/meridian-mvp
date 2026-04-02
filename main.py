
import os, io, re, html, hashlib, random, secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import Json, RealDictCursor

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATABASE_URL = os.getenv("DATABASE_URL")
APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:8000")
SESSION_SECRET = os.getenv("SESSION_SECRET", "meridian-dev-session-secret-change-me")
ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "red123")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required for the Postgres build.")

pool = SimpleConnectionPool(1, 10, DATABASE_URL)

def get_conn(): return pool.getconn()
def put_conn(conn): pool.putconn(conn)
def make_gmid(seed: str) -> str: return hashlib.sha256((seed + "|MERIDIAN_PG_V3").encode("utf-8")).hexdigest()
def hash_password(password: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), SESSION_SECRET.encode("utf-8"), 150000).hex()

def verify_password(password: str, password_hash: str) -> bool:
    return hash_password(password) == (password_hash or "")

def slugify_username(value: str) -> str:
    base = re.sub(r"[^a-z0-9]+", ".", (value or "").lower()).strip(".")
    return base or "member"

def unique_username(cur, desired: str, current_member_id: Optional[int] = None) -> str:
    base = slugify_username(desired)
    candidate = base
    counter = 1
    while True:
        if current_member_id is None:
            cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s)", (candidate,))
        else:
            cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s) AND member_id<>%s", (candidate, current_member_id))
        row = cur.fetchone()
        if not row:
            return candidate
        counter += 1
        candidate = f"{base}{counter}"


ALIAS_ADJ = ["Blue","Red","Green","Swift","Bold","Wise","Fierce","Calm","Silent","Brave","Mystic","Dark","Light","Stormy","Fiery","Icy","Golden","Silver","Shadow","Thunder","Ancient","Eternal","Vivid","Quiet","Loud","Sharp","Dull","Bright","Dim","Hot","Cold","Wet","Dry","Fast","Slow","Heavy","Strong","Weak","Tall","Short","Long","Brief","Deep","Shallow","Wide","Narrow","Old","New","Young","Aged","Pure","Tainted","Clear","Cloudy","Sunny","Rainy","Windy","Still","Wild","Tame","Free","Bound","Happy","Sad","Angry","Peaceful","Chaotic","Orderly","Elegant","Clumsy","Graceful","Awkward","Smart","Clever","Rich","Poor","Full","Empty","Open","Closed","Locked","Unlocked","Safe","Dangerous","Friendly","Hostile","Warm","Cool","Soft","Hard","Smooth","Rough","Shiny","Matte","Vibrant","Faded","Iron","Velvet"]
ALIAS_NOUN = ["Dragon","Phoenix","Tiger","Eagle","Wolf","Fox","Bear","Lion","Hawk","Raven","Shark","Panther","Owl","Falcon","Viper","Cobra","Lynx","Stag","Bull","Horse","Snake","Spider","Scorpion","Whale","Dolphin","Fish","Bird","Cat","Dog","Mouse","Bat","Deer","Elk","Moose","Rabbit","Hare","Squirrel","Beaver","Otter","Seal","Walrus","Penguin","Ostrich","Peacock","Parrot","Crow","Dove","Swan","Goose","Duck","Pig","Cow","Sheep","Goat","Donkey","Mule","Camel","Llama","Elephant","Rhino","Hippo","Giraffe","Zebra","Antelope","Buffalo","Bison","Yak","Monkey","Ape","Gorilla","Chimp","Lemur","Sloth","Koala","Kangaroo","Platypus","Turtle","Tortoise","Lizard","Gecko","Iguana","Alligator","Crocodile","Frog","Toad","Salamander","Newt","Butterfly","Moth","Bee","Wasp","Condor","Manta","Narwhal","Puma","Jaguar","Raptor","Coyote","Badger"]

def alias_from_gmid(gmid: str) -> str:
    c = re.sub(r"[^a-fA-F0-9]", "", gmid or "").lower().ljust(16, "0")
    return ALIAS_ADJ[int(c[:8], 16) % len(ALIAS_ADJ)] + ALIAS_NOUN[int(c[8:16], 16) % len(ALIAS_NOUN)]

def demo_login_username_for_gmid(gmid: str) -> str:
    return slugify_username(alias_from_gmid(gmid))

def demo_username_for_member(display_name: str, is_system: bool, fallback_member_id: int) -> str:
    if is_system:
        m = re.search(r"EX-(\d{1,3})", display_name or "")
        if m:
            return f"member{int(m.group(1)):03d}"
        return f"member{int(fallback_member_id):03d}"
    return slugify_username(display_name or f"member.{fallback_member_id}")

def tokenize(text: str) -> List[str]:
    return [t.lower() for t in re.findall(r"[a-zA-Z0-9&\-/]+", text or "") if len(t) > 1]

def normalize_list(value):
    if value is None: return []
    if isinstance(value, list): items = value
    else: items = [x.strip() for x in str(value).split(",")]
    out, seen = [], set()
    for item in items:
        s = str(item).strip()
        if s and s.lower() not in seen:
            out.append(s); seen.add(s.lower())
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
        score += min(8, len([k for k,v in attrs.items() if v not in (None,"",[],{})]))
    return min(100, score)

def weighted_hits(query: str, items: List[str], exact_weight: int, token_weight: int) -> int:
    q = query.lower(); q_tokens = set(tokenize(query)); score = 0
    for item in items:
        s = str(item).lower()
        if s in q or q in s: score += exact_weight
        else: score += len(set(tokenize(s)) & q_tokens) * token_weight
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
    exp = int(p.get("experience_years") or 0); wanted = extract_years_from_query(query)
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
    if name.endswith((".txt",".md",".csv",".json")): return raw.decode("utf-8", errors="ignore")
    if name.endswith((".html",".htm")):
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
    text = (text or "").strip()
    lower = text.lower()
    doc_tokens = set(tokenize(text))
    lines = [re.sub(r"\s+", " ", x).strip(" -•\t") for x in re.split(r"[\n\r]+|[;]", text) if x and x.strip()]

    def unique_keep(items: List[str]) -> List[str]:
        out, seen = [], set()
        for item in items:
            s = str(item).strip()
            key = s.lower()
            if s and key not in seen:
                out.append(s)
                seen.add(key)
        return out

    def scan_keywords(candidates: List[str], exact: bool = False) -> List[str]:
        found = []
        for candidate in candidates:
            c = candidate.lower()
            c_tokens = set(tokenize(candidate))
            if c in lower:
                found.append(candidate)
            elif not exact and c_tokens and len(c_tokens & doc_tokens) >= max(1, min(2, len(c_tokens))):
                found.append(candidate)
        return unique_keep(found)

    domains = scan_keywords(DOMAIN_KEYWORDS)
    roles = scan_keywords(ROLE_KEYWORDS)
    networks = scan_keywords(NETWORK_KEYWORDS)
    values = scan_keywords(VALUE_KEYWORDS, exact=True)

    years = max([int(x) for x in re.findall(r"(\d+)\+?\s+years?", lower)] + [0])
    if not years:
        m = re.search(r"experience[^\n\r]{0,30}?(\d{1,2})", lower)
        if m:
            years = int(m.group(1))

    assets, biography_parts, political_social = [], [], []
    leadership_verbs = ["led","built","delivered","improved","implemented","executed","created","managed","launched","reduced","increased","designed","negotiated","supported","stabilized","scaled","advised","transformed","owned","grew"]
    social_terms = ["board","advisor","advisory","conference","speaker","nonprofit","fundraising","campaign","alumni","community","policy","government","association","committee","forum"]
    for line in lines:
        ll = line.lower()
        if len(line) >= 18 and any(w in ll for w in leadership_verbs):
            assets.append(line)
        elif 30 <= len(line) <= 260 and len(biography_parts) < 4:
            biography_parts.append(line)
        if any(term in ll for term in social_terms) and len(line) >= 10:
            political_social.append(line)
    assets = unique_keep(assets)[:12]
    political_social = unique_keep(political_social)[:8]

    headline = None
    for line in lines[:8]:
        if 8 <= len(line) <= 120 and not re.search(r"@|\b(phone|email|linkedin|resume|curriculum vitae)\b", line, re.I):
            headline = line
            break
    if not headline:
        head_role = roles[0] if roles else None
        head_domain = domains[0] if domains else None
        headline = f"{head_role} · {head_domain}" if head_role and head_domain else (head_role or head_domain or None)

    biography = " ".join(unique_keep(biography_parts))[:1200] or None
    attributes = {
        "sap": "sap" in lower,
        "oracle": "oracle" in lower,
        "azure": "azure" in lower,
        "aws": "aws" in lower,
        "board": "board" in lower,
        "global": "global" in lower,
        "confidential": "confidential" in lower,
        "private_equity": "private equity" in lower,
        "venture_capital": "venture capital" in lower,
        "fortune_500": "fortune 500" in lower,
    }
    attributes = {k:v for k,v in attributes.items() if v}

    return {
        "headline": headline,
        "biography": biography,
        "domains": domains,
        "roles": roles,
        "networks": networks,
        "political_social": political_social,
        "values": values,
        "assets": assets,
        "experience_years": years,
        "attributes": attributes,
        "extracted_preview": text[:2000]
    }
def init_schema():
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS members (
              id BIGSERIAL PRIMARY KEY,
              gmid TEXT UNIQUE NOT NULL,
              display_name TEXT NOT NULL,
              email TEXT,
              is_system BOOLEAN NOT NULL DEFAULT FALSE,
              status TEXT NOT NULL DEFAULT 'active',
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS member_auth (
              member_id BIGINT PRIMARY KEY REFERENCES members(id) ON DELETE CASCADE,
              username TEXT UNIQUE NOT NULL,
              password_hash TEXT NOT NULL,
              must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              last_login_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS member_profiles (
              member_id BIGINT PRIMARY KEY REFERENCES members(id) ON DELETE CASCADE,
              headline TEXT,
              biography TEXT,
              domains_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              roles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              experience_years INT NOT NULL DEFAULT 0,
              networks_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              political_social_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              assets_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              values_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              attributes_json JSONB NOT NULL DEFAULT '{}'::jsonb,
              strength_score INT NOT NULL DEFAULT 0,
              updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS member_documents (
              id BIGSERIAL PRIMARY KEY,
              member_id BIGINT NOT NULL REFERENCES members(id) ON DELETE CASCADE,
              filename TEXT NOT NULL,
              content_type TEXT,
              source_type TEXT,
              extracted_text TEXT,
              parsed_json JSONB NOT NULL DEFAULT '{}'::jsonb,
              uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS member_invitations (
              id BIGSERIAL PRIMARY KEY,
              candidate_name TEXT NOT NULL,
              candidate_email TEXT NOT NULL,
              reference_gmid TEXT NOT NULL,
              invited_by_gmid TEXT NOT NULL,
              invitation_token TEXT UNIQUE NOT NULL,
              invitation_status TEXT NOT NULL DEFAULT 'sent',
              invite_note TEXT,
              sent_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              accepted_at TIMESTAMPTZ,
              expires_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS member_references (
              id BIGSERIAL PRIMARY KEY,
              invitation_id BIGINT NOT NULL REFERENCES member_invitations(id) ON DELETE CASCADE,
              reference_gmid TEXT NOT NULL,
              sponsor_gmid TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'active',
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS pings (
              id BIGSERIAL PRIMARY KEY,
              requester_gmid TEXT NOT NULL,
              target_gmid TEXT NOT NULL,
              request_text TEXT NOT NULL,
              score INT NOT NULL,
              status TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              responded_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS chat_messages (
              id BIGSERIAL PRIMARY KEY,
              ping_id BIGINT NOT NULL REFERENCES pings(id) ON DELETE CASCADE,
              sender_gmid TEXT NOT NULL,
              message TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            ALTER TABLE member_documents ADD COLUMN IF NOT EXISTS source_type TEXT;
            UPDATE member_documents
               SET source_type = COALESCE(NULLIF(source_type, ''), 'upload')
             WHERE source_type IS NULL OR source_type = '';
            """)
            cur.execute("SELECT id, gmid FROM members WHERE display_name=%s", ("Mike S",))
            mike_row = cur.fetchone()
            if not mike_row:
                gmid = make_gmid("Mike S|PRINCIPAL")
                cur.execute("INSERT INTO members (gmid, display_name, email, is_system, status) VALUES (%s,%s,%s,%s,%s) RETURNING id, gmid", (gmid, "Mike S", "mike@meridian.local", False, "active"))
                mike_row = cur.fetchone()
                member_id = mike_row[0]
                profile = {"domains":["Private Equity","Financial Services","Executive Search","Strategic Introductions"],"roles":["Principal","Managing Partner"],"experience_years":18,"networks":["Global LP network","C-suite operator channel","Board-level advisory"],"assets":["20+ years executing discreet executive mandates","Deep LP and sovereign fund relationships","Multi-sector board and operating network","Cross-border deal origination track record","Non-attributable introduction protocol","Known for zero-ego, outcome-first execution"],"values":["Discretion","Reciprocity","Outcome rigor"],"attributes":{"engagement_type":"advisory","confidentiality":"non-attribution"}}
                cur.execute("""INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(member_id, Json(profile["domains"]), Json(profile["roles"]), profile["experience_years"], Json(profile["networks"]), Json(profile["assets"]), Json(profile["values"]), Json(profile["attributes"]), profile_strength_score(profile)))
            else:
                member_id = mike_row[0]

            cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s)", ("mike",))
            username_owner = cur.fetchone()
            if username_owner and username_owner[0] != member_id:
                cur.execute("UPDATE member_auth SET username=%s WHERE member_id=%s", (f"member.{member_id}", username_owner[0]))

            cur.execute("SELECT 1 FROM member_auth WHERE member_id=%s", (member_id,))
            if cur.fetchone():
                cur.execute("UPDATE member_auth SET username=%s, password_hash=%s, must_change_password=FALSE WHERE member_id=%s", ("mike", hash_password("red123"), member_id))
            else:
                cur.execute("INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)", (member_id, "mike", hash_password("red123"), False))
            cur.execute("SELECT COUNT(*) FROM members WHERE is_system = TRUE")
            if cur.fetchone()[0] == 0:
                first = ["Avery","Jordan","Riley","Casey","Morgan","Taylor","Quinn","Hayden","Parker","Rowan","Blake","Cameron","Drew","Emerson","Finley","Harper","Kai","Logan","Micah","Noel"]
                last  = ["Stone","Reed","Carter","Hayes","Brooks","Wells","Foster","Shaw","Bennett","Cole","Sullivan","Pierce","Vaughn","Donovan","Holland","Walsh","Hayward","Monroe","Kendall","Navarro"]
                for i in range(100):
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
                    profile = {"domains":domains,"roles":roles,"experience_years":rnd.randint(8,28),"networks":networks,"assets":assets,"values":values,"attributes":{"engagement_type":"advisory retainer","availability":"near-term"}}
                    cur.execute("""INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(member_id, Json(profile["domains"]), Json(profile["roles"]), profile["experience_years"], Json(profile["networks"]), Json(profile["assets"]), Json(profile["values"]), Json(profile["attributes"]), profile_strength_score(profile)))
            cur.execute("""UPDATE members
                           SET status='active'
                           WHERE lower(coalesce(email,''))='darrin.joncas@gmail.com'
                              OR lower(display_name)='darrin joncas'
                              OR id IN (SELECT member_id FROM member_auth WHERE lower(username)='darrin.joncas')""")
            cur.execute("""SELECT m.id, m.display_name, m.email, m.is_system, a.member_id, a.username, m.gmid
                           FROM members m
                           LEFT JOIN member_auth a ON a.member_id = m.id
                           ORDER BY m.is_system DESC, m.created_at ASC, m.id ASC""")
            for row in cur.fetchall():
                member_id = row[0]
                display_name = row[1]
                row_gmid = row[6] if len(row) > 6 else None
                email = row[2]
                is_system = bool(row[3])
                has_auth = row[4]
                existing_username = row[5]
                if member_id == mike_row[0]:
                    continue
                desired_username = demo_login_username_for_gmid(row_gmid) if is_system else (email.split("@")[0] if email else demo_username_for_member(display_name, False, member_id))
                if is_system:
                    desired_password = "red123"
                    must_change = False
                else:
                    desired_password = f"Meridian-{make_gmid(display_name)[:8]}"
                    must_change = True
                if has_auth:
                    if existing_username != desired_username:
                        safe_username = unique_username(cur, desired_username, member_id)
                        cur.execute(
                            "UPDATE member_auth SET username=%s, password_hash=%s, must_change_password=%s WHERE member_id=%s",
                            (safe_username, hash_password(desired_password), must_change, member_id)
                        )
                    else:
                        cur.execute(
                            "UPDATE member_auth SET password_hash=%s, must_change_password=%s WHERE member_id=%s",
                            (hash_password(desired_password), must_change, member_id)
                        )
                    continue
                username = unique_username(cur, desired_username)
                cur.execute(
                    "INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)",
                    (member_id, username, hash_password(desired_password), must_change)
                )
    finally:
        put_conn(conn)

app = FastAPI(title="Meridian Postgres", version="1.3")
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET, same_site="lax")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
def startup(): init_schema()

def fetch_profiles(limit: int = 250):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT m.gmid, m.display_name, m.email, m.is_system, m.status, m.created_at, p.headline, p.biography, p.domains_json AS domains, p.roles_json AS roles, p.experience_years, p.networks_json AS networks, p.political_social_json AS political_social, p.assets_json AS assets, p.values_json AS values, p.attributes_json AS attributes, p.strength_score FROM members m JOIN member_profiles p ON p.member_id = m.id ORDER BY m.created_at DESC LIMIT %s""", (limit,))
            return cur.fetchall()
    finally:
        put_conn(conn)

def fetch_member_pool(limit: int = 5000):
    return [p for p in fetch_profiles(limit) if p.get("status") in ("active", "pending_vetting") and p.get("status") != "ghosted"]

def compute_reputation_snapshot() -> Dict[str, Dict[str, float]]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                WITH ping_rollup AS (
                    SELECT gmid,
                           SUM(sent_count) AS sent_count,
                           SUM(accepted_sent_count) AS accepted_sent_count,
                           SUM(received_count) AS received_count,
                           SUM(accepted_received_count) AS accepted_received_count,
                           SUM(total_connections) AS total_connections
                      FROM (
                        SELECT requester_gmid AS gmid,
                               COUNT(*) AS sent_count,
                               COUNT(*) FILTER (WHERE status='accepted') AS accepted_sent_count,
                               0::INT AS received_count,
                               0::INT AS accepted_received_count,
                               COUNT(*) FILTER (WHERE status='accepted') AS total_connections
                          FROM pings
                         GROUP BY requester_gmid
                        UNION ALL
                        SELECT target_gmid AS gmid,
                               0::INT AS sent_count,
                               0::INT AS accepted_sent_count,
                               COUNT(*) AS received_count,
                               COUNT(*) FILTER (WHERE status='accepted') AS accepted_received_count,
                               COUNT(*) FILTER (WHERE status='accepted') AS total_connections
                          FROM pings
                         GROUP BY target_gmid
                      ) x
                     GROUP BY gmid
                ),
                ref_rollup AS (
                    SELECT reference_gmid AS gmid, COUNT(*) AS reference_count
                      FROM member_references
                     WHERE status='active'
                     GROUP BY reference_gmid
                )
                SELECT m.gmid,
                       COALESCE(pr.sent_count, 0) AS sent_count,
                       COALESCE(pr.accepted_sent_count, 0) AS accepted_sent_count,
                       COALESCE(pr.received_count, 0) AS received_count,
                       COALESCE(pr.accepted_received_count, 0) AS accepted_received_count,
                       COALESCE(pr.total_connections, 0) AS total_connections,
                       COALESCE(rr.reference_count, 0) AS reference_count
                  FROM members m
             LEFT JOIN ping_rollup pr ON pr.gmid = m.gmid
             LEFT JOIN ref_rollup rr ON rr.gmid = m.gmid
            """)
            rows = cur.fetchall()
    finally:
        put_conn(conn)
    snapshot = {}
    for row in rows:
        sent = int(row.get("sent_count") or 0)
        accepted_sent = int(row.get("accepted_sent_count") or 0)
        received = int(row.get("received_count") or 0)
        accepted_received = int(row.get("accepted_received_count") or 0)
        connections = int(row.get("total_connections") or 0)
        references = int(row.get("reference_count") or 0)
        sent_accept_rate = (accepted_sent / sent) if sent else 0.0
        received_accept_rate = (accepted_received / received) if received else 0.0
        reputation = min(100.0, round((sent_accept_rate * 42.0) + (received_accept_rate * 28.0) + (min(connections, 12) * 1.75) + (min(references, 8) * 1.5), 2))
        snapshot[row["gmid"]] = {
            "sent_count": sent,
            "accepted_sent_count": accepted_sent,
            "received_count": received,
            "accepted_received_count": accepted_received,
            "connections": connections,
            "reference_count": references,
            "sent_accept_rate": round(sent_accept_rate, 4),
            "received_accept_rate": round(received_accept_rate, 4),
            "reputation_score": reputation,
        }
    return snapshot

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if request.session.get("member_gmid"):
        return RedirectResponse(url="/me", status_code=302)
    return HTMLResponse(open(os.path.join(BASE_DIR, "ui.html"), "r", encoding="utf-8").read())

@app.get("/member/{gmid}", response_class=HTMLResponse)
def member(request: Request, gmid: str):
    session_gmid = request.session.get("member_gmid")
    if not session_gmid:
        return RedirectResponse(url="/", status_code=302)
    if session_gmid != gmid:
        return RedirectResponse(url=f"/member/{session_gmid}", status_code=302)
    return HTMLResponse(open(os.path.join(BASE_DIR, "member.html"), "r", encoding="utf-8").read().replace("{{GMID}}", gmid))

@app.get("/me")
def my_home(request: Request):
    gmid = request.session.get("member_gmid")
    if not gmid:
        return RedirectResponse(url="/", status_code=302)
    query = str(request.url.query or "").strip()
    suffix = f"?{query}" if query else ""
    return RedirectResponse(url=f"/member/{gmid}{suffix}", status_code=302)

@app.get("/rankings", response_class=HTMLResponse)
def rankings_page(): return HTMLResponse(open(os.path.join(BASE_DIR, "rankings.html"), "r", encoding="utf-8").read())

@app.get("/alias", response_class=HTMLResponse)
def alias_page(): return HTMLResponse(open(os.path.join(BASE_DIR, "GMID.html"), "r", encoding="utf-8").read())

@app.get("/profile/edit", response_class=HTMLResponse)
def profile_edit_page(request: Request):
    gmid = request.session.get("member_gmid")
    if not gmid:
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(open(os.path.join(BASE_DIR, "profile_editor.html"), "r", encoding="utf-8").read().replace("{{GMID}}", gmid))

@app.get("/members", response_class=HTMLResponse)
def members_page(request: Request):
    if not request.session.get("member_gmid"):
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(open(os.path.join(BASE_DIR, "members.html"), "r", encoding="utf-8").read())

@app.get("/invite-member", response_class=HTMLResponse)
def invite_member_page(request: Request):
    if not request.session.get("member_gmid"):
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(open(os.path.join(BASE_DIR, "invite_member.html"), "r", encoding="utf-8").read())

@app.get("/invite/{token}", response_class=HTMLResponse)
def complete_profile_page(token: str): return HTMLResponse(open(os.path.join(BASE_DIR, "complete_profile.html"), "r", encoding="utf-8").read().replace("{{TOKEN}}", token))

@app.get("/admin-login", response_class=HTMLResponse)
def admin_login_page():
    return HTMLResponse(open(os.path.join(BASE_DIR, "admin_login.html"), "r", encoding="utf-8").read())

@app.get("/admin", response_class=HTMLResponse)
def admin_page(request: Request):
    if not request.session.get("is_admin"):
        return RedirectResponse(url="/admin-login", status_code=302)
    return HTMLResponse(open(os.path.join(BASE_DIR, "admin_members.html"), "r", encoding="utf-8").read())


def get_current_admin(request: Request) -> bool:
    return bool(request.session.get("is_admin"))


def get_current_member(request: Request):
    gmid = request.session.get("member_gmid")
    if not gmid:
        return None
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT gmid, display_name, email, status, is_system FROM members WHERE gmid=%s AND status IN ('active','pending_vetting')", (gmid,))
            return cur.fetchone()
    finally:
        put_conn(conn)

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    member = get_current_member(request)
    return JSONResponse(content=jsonable_encoder({"ok": True, "authenticated": bool(member), "member": member}))

@app.get("/api/admin/me")
def api_admin_me(request: Request):
    return JSONResponse(content={"ok": True, "authenticated": bool(request.session.get("is_admin")), "username": request.session.get("admin_username")})

@app.post("/api/admin/login")
async def api_admin_login(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if username != ADMIN_USERNAME or password != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid admin credentials")
    request.session["is_admin"] = True
    request.session["admin_username"] = ADMIN_USERNAME
    return JSONResponse(content={"ok": True, "redirect_url": "/admin"})

@app.post("/api/admin/logout")
def api_admin_logout(request: Request):
    request.session.pop("is_admin", None)
    request.session.pop("admin_username", None)
    return JSONResponse(content={"ok": True})

@app.post("/api/auth/login")
async def api_auth_login(request: Request):
    data = await request.json()
    username = (data.get("username") or "").strip().lower()
    password = data.get("password") or ""
    if not username or not password:
        raise HTTPException(status_code=400, detail="username and password required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT a.username, a.password_hash, a.must_change_password, m.gmid, m.display_name, m.email, m.status
                           FROM member_auth a
                           JOIN members m ON m.id = a.member_id
                           WHERE lower(a.username)=lower(%s)""", (username,))
            row = cur.fetchone()
            if not row or row["status"] not in ("active", "pending_vetting") or not verify_password(password, row["password_hash"]):
                raise HTTPException(status_code=401, detail="Invalid credentials")
            request.session["member_gmid"] = row["gmid"]
            request.session["username"] = row["username"]
            cur.execute("UPDATE member_auth SET last_login_at=NOW() WHERE username=%s", (row["username"],))
            return JSONResponse(content=jsonable_encoder({"ok": True, "member": {"gmid": row["gmid"], "display_name": row["display_name"], "email": row["email"], "status": row["status"]}, "redirect_url": f"/member/{row['gmid']}", "must_change_password": row["must_change_password"]}))
    finally:
        put_conn(conn)

@app.post("/api/auth/logout")
def api_auth_logout(request: Request):
    request.session.clear()
    return JSONResponse(content={"ok": True})

@app.post("/api/auth/change-password")
async def api_auth_change_password(request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = await request.json()
    current_password = data.get("current_password") or ""
    new_password = data.get("new_password") or ""
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT username, password_hash FROM member_auth a JOIN members m ON m.id=a.member_id WHERE m.gmid=%s", (member["gmid"],))
            row = cur.fetchone()
            if not row or not verify_password(current_password, row["password_hash"]):
                raise HTTPException(status_code=401, detail="Current password is incorrect")
            cur.execute("UPDATE member_auth SET password_hash=%s, must_change_password=FALSE WHERE username=%s", (hash_password(new_password), row["username"]))
            return JSONResponse(content={"ok": True})
    finally:
        put_conn(conn)

@app.get("/api/profiles")
def api_profiles(limit: int = 250): return JSONResponse(content=jsonable_encoder({"count": limit, "profiles": fetch_profiles(limit)}))

@app.get("/api/profile/{gmid}")
def api_profile(gmid: str):
    current = next((p for p in fetch_profiles(5000) if p["gmid"] == gmid), None)
    if not current: raise HTTPException(status_code=404, detail="profile not found")
    return JSONResponse(content=jsonable_encoder({"ok": True, "profile": current}))

@app.get("/api/profile/me")
def api_profile_me(request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT m.gmid, m.display_name, m.email, m.status, m.is_system,
                                  p.headline, p.biography, p.domains_json AS domains, p.roles_json AS roles,
                                  p.experience_years, p.networks_json AS networks, p.political_social_json AS political_social,
                                  p.assets_json AS assets, p.values_json AS values, p.attributes_json AS attributes,
                                  p.strength_score, p.updated_at
                           FROM members m
                           JOIN member_profiles p ON p.member_id = m.id
                           WHERE m.gmid=%s""", (member["gmid"],))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="profile not found")
            cur.execute("""SELECT id, filename, COALESCE(source_type, content_type, 'upload') AS source_type, parsed_json, uploaded_at
                           FROM member_documents md
                           JOIN members m ON m.id = md.member_id
                           WHERE m.gmid=%s
                           ORDER BY uploaded_at DESC
                           LIMIT 10""", (member["gmid"],))
            docs = cur.fetchall()
            return JSONResponse(content=jsonable_encoder({"ok": True, "profile": row, "documents": docs, "alias": alias_from_gmid(member["gmid"])}))
    finally:
        put_conn(conn)

@app.post("/api/profile/me")
async def api_profile_me_update(request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = await request.json()
    display_name = (data.get("display_name") or member["display_name"] or "").strip() or member["display_name"]
    email = (data.get("email") or member.get("email") or "").strip().lower() or None
    headline = (data.get("headline") or "").strip() or None
    biography = (data.get("biography") or "").strip() or None
    domains = normalize_list(data.get("domains"))
    roles = normalize_list(data.get("roles"))
    networks = normalize_list(data.get("networks"))
    political_social = normalize_list(data.get("political_social"))
    assets = normalize_list(data.get("assets"))
    values = normalize_list(data.get("values"))
    experience_years = int(data.get("experience_years") or 0)
    attributes = data.get("attributes") or {}
    profile = {"domains": domains, "roles": roles, "experience_years": experience_years, "networks": networks, "assets": assets, "values": values, "attributes": attributes}
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM members WHERE gmid=%s", (member["gmid"],))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="member not found")
            member_id = row["id"]
            cur.execute("UPDATE members SET display_name=%s, email=%s, status='active' WHERE id=%s", (display_name, email, member_id))
            cur.execute("""UPDATE member_profiles
                           SET headline=%s, biography=%s, domains_json=%s, roles_json=%s, experience_years=%s,
                               networks_json=%s, political_social_json=%s, assets_json=%s, values_json=%s,
                               attributes_json=%s, strength_score=%s, updated_at=NOW()
                           WHERE member_id=%s""",
                        (headline, biography, Json(domains), Json(roles), experience_years, Json(networks), Json(political_social), Json(assets), Json(values), Json(attributes), profile_strength_score(profile), member_id))
        return JSONResponse(content=jsonable_encoder({"ok": True, "status": "active"}))
    finally:
        put_conn(conn)

@app.post("/api/profile/me/upload")
async def api_profile_me_upload(request: Request, document: UploadFile = File(...)):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    raw = await document.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")
    extracted = extract_text_from_upload(document, raw)
    parsed = parse_profile_text(extracted)
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id FROM members WHERE gmid=%s", (member["gmid"],))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="member not found")
            member_id = row["id"]
            cur.execute("""INSERT INTO member_documents (member_id, filename, content_type, source_type, extracted_text, parsed_json)
                           VALUES (%s,%s,%s,%s,%s,%s) RETURNING id, uploaded_at""",
                        (member_id, document.filename or "upload", document.content_type or "application/octet-stream", "upload", extracted, Json(parsed)))
            saved = cur.fetchone()
        return JSONResponse(content=jsonable_encoder({
            "ok": True,
            "document_id": saved["id"],
            "uploaded_at": saved["uploaded_at"],
            "filename": document.filename or "upload",
            "parsed": parsed,
            "extracted_preview": parsed.get("extracted_preview", ""),
            "message": "Document parsed. Review and save the mapped profile fields."
        }))
    finally:
        put_conn(conn)


@app.post("/api/profile/create")
async def api_profile_create(request: Request):
    data = await request.json()
    display_name = (data.get("display_name") or "").strip()
    if not display_name:
        raise HTTPException(status_code=400, detail="display_name required")
    domains = normalize_list(data.get("domains"))
    roles = normalize_list(data.get("roles"))
    networks = normalize_list(data.get("networks"))
    political_social = normalize_list(data.get("political_social"))
    assets = normalize_list(data.get("assets"))
    values = normalize_list(data.get("values"))
    experience_years = int(data.get("experience_years") or 0)
    email = (data.get("email") or "").strip().lower() or None
    gmid = make_gmid(display_name + "|" + (email or display_name))
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT gmid FROM members WHERE gmid=%s", (gmid,))
            existing = cur.fetchone()
            if existing:
                return JSONResponse(content=jsonable_encoder({"ok": True, "gmid": existing["gmid"], "created": False}))
            cur.execute("INSERT INTO members (gmid, display_name, email, is_system, status) VALUES (%s,%s,%s,FALSE,'active') RETURNING id", (gmid, display_name, email))
            member_id = cur.fetchone()["id"]
            profile = {"domains":domains,"roles":roles,"experience_years":experience_years,"networks":networks,"assets":assets,"values":values,"attributes":{}}
            username = unique_username(cur, data.get("username") or (email.split("@")[0] if email else display_name))
            password = data.get("password") or f"Meridian-{gmid[:8]}"
            cur.execute("""INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, political_social_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (member_id, Json(domains), Json(roles), experience_years, Json(networks), Json(political_social), Json(assets), Json(values), Json({}), profile_strength_score(profile)))
            cur.execute("INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)", (member_id, username, hash_password(password), True))
            return JSONResponse(content=jsonable_encoder({"ok": True, "gmid": gmid, "created": True, "username": username, "temporary_password": password}))
    finally:
        put_conn(conn)

@app.post("/api/match")
async def api_match(request: Request):
    data = await request.json(); q = (data.get("query") or "").strip(); requester = (data.get("requester_gmid") or "").strip()
    if not q: raise HTTPException(status_code=400, detail="query is required")
    canonical_pool = fetch_member_pool(5000)
    pool = [p for p in canonical_pool if p["gmid"] != requester]
    reputation = compute_reputation_snapshot()
    requester_profile = next((p for p in canonical_pool if p["gmid"] == requester), None)
    requester_domains = set((requester_profile or {}).get("domains") or [])
    requester_networks = set((requester_profile or {}).get("networks") or [])
    scored = []
    for p in pool:
        base = score_profile(q, p)
        rep = reputation.get(p["gmid"], {})
        domain_overlap = len(requester_domains & set(p.get("domains") or []))
        network_overlap = len(requester_networks & set(p.get("networks") or []))
        elite_score = base + min(12, domain_overlap * 4) + min(10, network_overlap * 5) + ((rep.get("reputation_score") or 0) * 0.18)
        if elite_score > 0:
            scored.append((elite_score, p, rep, domain_overlap, network_overlap))
    scored.sort(key=lambda x:(x[0], profile_strength_score(x[1]), x[2].get("reputation_score", 0)), reverse=True)
    out=[]
    for s,p,rep,domain_overlap,network_overlap in scored[:10]:
        out.append({
            "score": int(round(s)),
            "profile": {
                "gmid": p["gmid"],
                "display_name": p["display_name"],
                "domains": p.get("domains") or [],
                "roles": p.get("roles") or [],
                "experience_years": p.get("experience_years") or 0,
                "assets_preview": (p.get("assets") or [])[:6],
                "networks": p.get("networks") or [],
                "is_system": p.get("is_system"),
                "strength_score": p.get("strength_score") or profile_strength_score(p),
                "reputation_score": rep.get("reputation_score", 0),
                "headline": p.get("headline")
            },
            "signals": {
                "shared_domains": domain_overlap,
                "shared_networks": network_overlap,
                "connections": rep.get("connections", 0),
                "references": rep.get("reference_count", 0)
            }
        })
    return JSONResponse(content=jsonable_encoder({"ok": True, "query": q, "count": len(out), "results": out, "pool": "canonical_active_pending_member_pool"}))

@app.post("/api/ping")
async def api_ping(request: Request):
    data = await request.json()
    requester = (data.get("requester_gmid") or "").strip()
    target = (data.get("target_gmid") or "").strip()
    txt = (data.get("request_text") or "").strip()
    score = int(data.get("score") or 0)
    if not requester or not target or not txt: raise HTTPException(status_code=400, detail="requester_gmid, target_gmid, request_text required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("INSERT INTO pings (requester_gmid, target_gmid, request_text, score, status) VALUES (%s,%s,%s,%s,'pending') RETURNING id", (requester, target, txt, score))
            return JSONResponse(content=jsonable_encoder({"ok": True, "ping_id": cur.fetchone()["id"]}))
    finally: put_conn(conn)

@app.get("/api/inbox/{gmid}")
def api_inbox(gmid: str, limit: int = 200):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.*, req.display_name AS requester_name, tgt.display_name AS target_name FROM pings p LEFT JOIN members req ON req.gmid=p.requester_gmid LEFT JOIN members tgt ON tgt.gmid=p.target_gmid WHERE p.target_gmid=%s ORDER BY p.created_at DESC LIMIT %s""", (gmid, limit))
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": cur.fetchall()}))
    finally: put_conn(conn)

@app.get("/api/outbox/{gmid}")
def api_outbox(gmid: str, limit: int = 200):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.*, req.display_name AS requester_name, tgt.display_name AS target_name FROM pings p LEFT JOIN members req ON req.gmid=p.requester_gmid LEFT JOIN members tgt ON tgt.gmid=p.target_gmid WHERE p.requester_gmid=%s ORDER BY p.created_at DESC LIMIT %s""", (gmid, limit))
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": cur.fetchall()}))
    finally: put_conn(conn)

@app.post("/api/ping/{ping_id}/respond")
async def api_respond(ping_id: int, request: Request):
    data = await request.json(); status = (data.get("status") or "").strip().lower()
    if status not in ("accepted","declined"): raise HTTPException(status_code=400, detail="status must be accepted or declined")
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("UPDATE pings SET status=%s, responded_at=NOW() WHERE id=%s", (status, ping_id))
        return JSONResponse(content=jsonable_encoder({"ok": True, "ping_id": ping_id, "status": status}))
    finally: put_conn(conn)

@app.get("/api/chat/{ping_id}")
def api_chat(ping_id: int):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, requester_gmid, target_gmid, status FROM pings WHERE id=%s", (ping_id,))
            ping = cur.fetchone()
            if not ping: raise HTTPException(status_code=404, detail="ping not found")
            if ping["status"] != "accepted": raise HTTPException(status_code=400, detail="chat available only after accept")
            cur.execute("SELECT * FROM chat_messages WHERE ping_id=%s ORDER BY id ASC", (ping_id,))
            return JSONResponse(content=jsonable_encoder({"ok": True, "ping": ping, "messages": cur.fetchall()}))
    finally: put_conn(conn)

@app.post("/api/chat/{ping_id}/send")
async def api_chat_send(ping_id: int, request: Request):
    data = await request.json(); sender=(data.get("sender_gmid") or "").strip(); msg=(data.get("message") or "").strip()
    if not sender or not msg: raise HTTPException(status_code=400, detail="sender_gmid and message required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT status FROM pings WHERE id=%s", (ping_id,))
            ping = cur.fetchone()
            if not ping: raise HTTPException(status_code=404, detail="ping not found")
            if ping["status"] != "accepted": raise HTTPException(status_code=400, detail="chat available only after accept")
            cur.execute("INSERT INTO chat_messages (ping_id, sender_gmid, message) VALUES (%s,%s,%s)", (ping_id, sender, msg))
        return JSONResponse(content=jsonable_encoder({"ok": True}))
    finally: put_conn(conn)

@app.get("/api/network/{gmid}")
def api_network(gmid: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.*, req.display_name AS requester_name, tgt.display_name AS target_name FROM pings p LEFT JOIN members req ON req.gmid=p.requester_gmid LEFT JOIN members tgt ON tgt.gmid=p.target_gmid WHERE p.status='accepted' AND (p.requester_gmid=%s OR p.target_gmid=%s) ORDER BY COALESCE(p.responded_at, p.created_at) DESC, p.id DESC""", (gmid, gmid))
            rows = cur.fetchall()
            nodes, edges, seen = [], [], set()
            def add_node(node_gmid, label):
                if node_gmid not in seen:
                    seen.add(node_gmid); nodes.append({"gmid": node_gmid, "label": label})
            add_node(gmid, "Me")
            for r in rows:
                add_node(r["requester_gmid"], r["requester_name"] or r["requester_gmid"])
                add_node(r["target_gmid"], r["target_name"] or r["target_gmid"])
                other = r["target_gmid"] if r["requester_gmid"] == gmid else r["requester_gmid"]
                other_name = r["target_name"] if r["requester_gmid"] == gmid else r["requester_name"]
                edges.append({"ping_id": r["id"], "other_gmid": other, "other_name": other_name, "created_at": r["created_at"], "responded_at": r["responded_at"]})
            return JSONResponse(content=jsonable_encoder({"ok": True, "center_gmid": gmid, "nodes": nodes, "edges": edges}))
    finally: put_conn(conn)

@app.get("/api/rankings")
def api_rankings(limit: int = 60):
    profiles = fetch_member_pool(5000)
    reputation = compute_reputation_snapshot()
    out=[]
    for p in profiles:
        rep = reputation.get(p["gmid"], {})
        strength = p.get("strength_score") or profile_strength_score(p)
        composite = round((strength * 0.58) + ((rep.get("reputation_score") or 0) * 0.32) + (min(rep.get("connections", 0), 12) * 1.1), 2)
        out.append({
            "gmid": p["gmid"],
            "display_name": p["display_name"],
            "strength_score": strength,
            "connections": rep.get("connections", 0),
            "response_rate": round(rep.get("sent_accept_rate", 0), 2),
            "reputation_score": rep.get("reputation_score", 0),
            "reference_count": rep.get("reference_count", 0),
            "composite_score": composite,
            "domains": p.get("domains") or []
        })
    out.sort(key=lambda x:(x["composite_score"], x["reputation_score"], x["strength_score"], x["connections"]), reverse=True)
    for idx,row in enumerate(out, start=1): row["rank"]=idx
    return JSONResponse(content=jsonable_encoder({"ok": True, "items": out[:limit], "pool": "canonical_active_pending_member_pool"}))

@app.post("/api/invitations/create")
async def api_create_invitation(request: Request):
    data = await request.json()
    candidate_name=(data.get("candidate_name") or "").strip(); candidate_email=(data.get("candidate_email") or "").strip().lower(); reference_gmid=(data.get("reference_gmid") or "").strip(); invited_by_gmid=(data.get("invited_by_gmid") or "").strip(); invite_note=(data.get("invite_note") or "").strip()
    if not candidate_name or not candidate_email or not reference_gmid or not invited_by_gmid: raise HTTPException(status_code=400, detail="candidate_name, candidate_email, reference_gmid, invited_by_gmid required")
    token = secrets.token_urlsafe(24); expires_at = datetime.now(timezone.utc) + timedelta(days=14)
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT gmid FROM members WHERE gmid=%s AND status='active'", (reference_gmid,))
            if not cur.fetchone(): raise HTTPException(status_code=400, detail="Reference GMID is not an active community member.")
            cur.execute("SELECT id FROM members WHERE lower(email)=lower(%s)", (candidate_email,))
            if cur.fetchone(): raise HTTPException(status_code=400, detail="That email already belongs to an existing Meridian member.")
            cur.execute("INSERT INTO member_invitations (candidate_name, candidate_email, reference_gmid, invited_by_gmid, invitation_token, invite_note, expires_at) VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id", (candidate_name, candidate_email, reference_gmid, invited_by_gmid, token, invite_note, expires_at))
            invitation_id = cur.fetchone()["id"]
            cur.execute("INSERT INTO member_references (invitation_id, reference_gmid, sponsor_gmid) VALUES (%s,%s,%s)", (invitation_id, reference_gmid, invited_by_gmid))
        return JSONResponse(content=jsonable_encoder({"ok": True, "invitation_id": invitation_id, "token": token, "invite_link": f"{APP_BASE_URL}/invite/{token}", "email_subject": "Congratulations — You’ve Been Invited to Join Meridian", "email_body": f"Congratulations — You’ve been invited to join Meridian. This invitation was sponsored by a member of the community. Complete your vetted profile here: {APP_BASE_URL}/invite/{token}"}))
    finally: put_conn(conn)

@app.get("/api/invitations/{token}")
def api_get_invitation(token: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM member_invitations WHERE invitation_token=%s", (token,))
            row = cur.fetchone()
            if not row: raise HTTPException(status_code=404, detail="Invitation not found")
            return JSONResponse(content=jsonable_encoder({"ok": True, "invitation": row}))
    finally: put_conn(conn)

@app.post("/api/invitations/{token}/complete")
async def api_complete_invitation(token: str, request: Request):
    data = await request.json()
    display_name=(data.get("display_name") or "").strip(); email=(data.get("email") or "").strip().lower()
    if not display_name or not email: raise HTTPException(status_code=400, detail="display_name and email required")
    domains=normalize_list(data.get("domains")); roles=normalize_list(data.get("roles")); networks=normalize_list(data.get("networks")); values=normalize_list(data.get("values")); assets=normalize_list(data.get("assets")); experience_years=int(data.get("experience_years") or 0); attributes=data.get("attributes") or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM member_invitations WHERE invitation_token=%s AND invitation_status='sent'", (token,))
            inv = cur.fetchone()
            if not inv: raise HTTPException(status_code=404, detail="Invitation not found or already completed")
            cur.execute("SELECT id FROM members WHERE lower(email)=lower(%s)", (email,))
            if cur.fetchone(): raise HTTPException(status_code=400, detail="That email already belongs to an existing Meridian member.")
            gmid = make_gmid(display_name + "|" + email)
            cur.execute("INSERT INTO members (gmid, display_name, email, is_system, status) VALUES (%s,%s,%s,FALSE,'pending_vetting') RETURNING id", (gmid, display_name, email))
            member_id = cur.fetchone()["id"]
            profile = {"domains":domains,"roles":roles,"experience_years":experience_years,"networks":networks,"assets":assets,"values":values,"attributes":attributes}
            username = unique_username(cur, username or email.split("@")[0])
            password = password or f"Meridian-{gmid[:8]}"
            cur.execute("INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (member_id, Json(domains), Json(roles), experience_years, Json(networks), Json(assets), Json(values), Json(attributes), profile_strength_score(profile)))
            cur.execute("UPDATE member_invitations SET invitation_status='accepted', accepted_at=NOW() WHERE id=%s", (inv["id"],))
            cur.execute("INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)", (member_id, username, hash_password(password), False))
        return JSONResponse(content=jsonable_encoder({"ok": True, "gmid": gmid, "status": "pending_vetting", "username": username}))
    finally: put_conn(conn)

@app.get("/api/members/discover")
def api_member_discovery(limit: int = 120):
    profiles = fetch_member_pool(5000)
    reputation = compute_reputation_snapshot()
    items = []
    for p in profiles:
        rep = reputation.get(p["gmid"], {})
        items.append({
            "gmid": p["gmid"],
            "alias": alias_from_gmid(p["gmid"]),
            "headline": p.get("headline") or (", ".join((p.get("roles") or [])[:2]) or "Meridian Member"),
            "domains": p.get("domains") or [],
            "roles": p.get("roles") or [],
            "networks": p.get("networks") or [],
            "experience_years": p.get("experience_years") or 0,
            "strength_score": p.get("strength_score") or profile_strength_score(p),
            "reputation_score": rep.get("reputation_score", 0),
            "connections": rep.get("connections", 0),
            "reference_count": rep.get("reference_count", 0)
        })
    items.sort(key=lambda x: (x["reputation_score"], x["strength_score"], x["experience_years"]), reverse=True)
    return JSONResponse(content=jsonable_encoder({"ok": True, "items": items[:limit], "pool": "canonical_active_pending_member_pool"}))

@app.get("/api/admin/members")
@app.get("/api/admin/members")
def api_admin_members(request: Request, limit: int = 500):
    if not get_current_admin(request):
        raise HTTPException(status_code=401, detail="Admin authentication required")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT m.id, m.gmid, m.display_name, m.email, m.is_system, m.status, a.username, a.must_change_password, a.last_login_at, m.created_at
                           FROM members m
                           LEFT JOIN member_auth a ON a.member_id = m.id
                           ORDER BY m.is_system DESC, m.created_at ASC, m.id ASC
                           LIMIT %s""", (limit,))
            rows = cur.fetchall()
            items = []
            for row in rows:
                alias_name = alias_from_gmid(row["gmid"])
                items.append({
                    "id": row["id"],
                    "gmid": row["gmid"],
                    "alias": alias_name,
                    "display_name": row["display_name"],
                    "email": row["email"],
                    "is_system": row["is_system"],
                    "status": row["status"],
                    "username": row["username"],
                    "password_hint": "red123" if row["is_system"] else None,
                    "must_change_password": row["must_change_password"],
                    "last_login_at": row["last_login_at"],
                    "created_at": row["created_at"]
                })
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": items, "admin_username": ADMIN_USERNAME}))
    finally:
        put_conn(conn)


@app.post("/api/admin/members/{member_id}/delete")
def api_admin_delete_member(member_id: int, request: Request):
    if not get_current_admin(request):
        raise HTTPException(status_code=401, detail="Admin authentication required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, gmid, display_name, status FROM members WHERE id=%s", (member_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Member not found")
            if row["status"] == "ghosted":
                return JSONResponse(content=jsonable_encoder({"ok": True, "ghosted": True, "member_id": member_id, "gmid": row["gmid"]}))
            cur.execute("DELETE FROM member_auth WHERE member_id=%s", (member_id,))
            cur.execute("UPDATE member_profiles SET headline=%s, biography=%s, domains_json='[]'::jsonb, roles_json='[]'::jsonb, networks_json='[]'::jsonb, assets_json='[]'::jsonb, values_json='[]'::jsonb, attributes_json='{}'::jsonb, strength_score=0 WHERE member_id=%s", ('GHOST MEMBER', 'Ghosted member record retained to preserve historical network links.', member_id))
            cur.execute("UPDATE members SET display_name=%s, email=NULL, status='ghosted', is_system=FALSE WHERE id=%s", ('GHOST MEMBER', member_id))
            return JSONResponse(content=jsonable_encoder({"ok": True, "ghosted": True, "member_id": member_id, "gmid": row["gmid"], "display_name": 'GHOST MEMBER'}))
    finally:
        put_conn(conn)

@app.get("/api/debug/db")
def api_debug():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM members")
            members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM members WHERE status IN ('active','pending_vetting') AND status <> 'ghosted'")
            community_members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM members WHERE status='active'")
            active_members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM members WHERE is_system = TRUE")
            system_members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM pings")
            pings = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM pings WHERE status='accepted'")
            accepted_pings = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM chat_messages")
            chats = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM member_invitations")
            invites = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM member_auth")
            auth_accounts = cur.fetchone()["c"]
            return JSONResponse(content=jsonable_encoder({
                "database": "postgres",
                "members": members,
                "profiles": members,
                "community_members": community_members,
                "active_members": active_members,
                "system_members": system_members,
                "demo_members": system_members,
                "pings": pings,
                "accepted_pings": accepted_pings,
                "chat_messages": chats,
                "invitations": invites,
                "auth_accounts": auth_accounts
            }))
    finally: put_conn(conn)
