
import os, io, re, html, hashlib, random, secrets, json
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


try:
    from openai import OpenAI
except Exception:
    OpenAI = None

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_PROJECT_ID = os.getenv("OPENAI_PROJECT_ID", "")


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

def canonical_alias(cur, gmid: str, member_id: Optional[int] = None) -> str:
    base = alias_from_gmid(gmid)
    alias_name = base
    n = 2
    while True:
        if member_id is None:
            cur.execute("SELECT id FROM members WHERE alias_name=%s", (alias_name,))
        else:
            cur.execute("SELECT id FROM members WHERE alias_name=%s AND id<>%s", (alias_name, member_id))
        row = cur.fetchone()
        if not row:
            return alias_name
        alias_name = f"{base}{n}"
        n += 1

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
    lower = (text or "").lower()
    domains = [d for d in DOMAIN_KEYWORDS if d.lower() in lower]
    roles = [r for r in ROLE_KEYWORDS if r.lower() in lower]
    networks = [n for n in NETWORK_KEYWORDS if n.lower() in lower]
    values = [v for v in VALUE_KEYWORDS if v.lower() in lower]
    years = max([int(x) for x in re.findall(r"(\d+)\+?\s+years?", lower)] + [0])
    lines = [re.sub(r"\s+", " ", x).strip(" -•\t") for x in re.split(r"[\n\r]+|[;]", text)]
    assets = []
    for line in lines:
        if len(line) < 18: continue
        if any(w in line.lower() for w in ["led","built","delivered","improved","implemented","executed","created","managed","launched","reduced","increased","designed","negotiated","supported","stabilized"]):
            if line not in assets: assets.append(line)
        if len(assets) >= 12: break
    attributes = {k: True for k in ["sap","oracle","azure","aws","board","global","confidential"] if k in lower}
    return {"domains": domains, "roles": roles, "networks": networks, "values": values, "assets": assets, "experience_years": years, "attributes": attributes, "extracted_preview": text[:1200]}

def match_dimension_scores(query: str, p: Dict[str, Any]) -> Dict[str, float]:
    exp = int(p.get("experience_years") or 0)
    wanted = extract_years_from_query(query)
    experience_score = max(0, 14 - min(14, abs(exp - wanted))) if wanted is not None else min(8, exp // 2)
    profile_quality = round(profile_strength_score(p) * 0.12, 2)
    return {
        "domains": weighted_hits(query, p.get("domains", []), 18, 6),
        "roles": weighted_hits(query, p.get("roles", []), 16, 5),
        "networks": weighted_hits(query, p.get("networks", []), 10, 3),
        "values": weighted_hits(query, p.get("values", []), 7, 2),
        "assets": weighted_hits(query, (p.get("assets") or [])[:20], 4, 1),
        "experience": experience_score,
        "profile_quality": profile_quality,
    }


def describe_match_reasons(query: str, p: Dict[str, Any]) -> List[str]:
    q_tokens = set(tokenize(query))
    reasons = []
    domains = p.get("domains") or []
    roles = p.get("roles") or []
    networks = p.get("networks") or []
    values = p.get("values") or []
    assets = p.get("assets") or []

    domain_hits = [d for d in domains if set(tokenize(d)) & q_tokens or d.lower() in query.lower()]
    role_hits = [r for r in roles if set(tokenize(r)) & q_tokens or r.lower() in query.lower()]
    network_hits = [n for n in networks if set(tokenize(n)) & q_tokens or n.lower() in query.lower()]
    value_hits = [v for v in values if set(tokenize(v)) & q_tokens or v.lower() in query.lower()]

    if domain_hits:
        reasons.append(f"Strong domain overlap in {', '.join(domain_hits[:3])}.")
    elif domains:
        reasons.append(f"Relevant domain coverage includes {', '.join(domains[:3])}.")

    if role_hits:
        reasons.append(f"Role alignment with {', '.join(role_hits[:2])}.")
    elif roles:
        reasons.append(f"Likely fit from role history including {', '.join(roles[:2])}.")

    exp = int(p.get("experience_years") or 0)
    if exp:
        reasons.append(f"Estimated experience depth: {exp} years.")

    if network_hits:
        reasons.append(f"Network overlap through {', '.join(network_hits[:2])}.")
    elif networks:
        reasons.append(f"Potential warm path via {', '.join(networks[:2])}.")

    if value_hits:
        reasons.append(f"Value signal match on {', '.join(value_hits[:2])}.")

    if assets:
        reasons.append(f"Proof point: {assets[0]}")

    return reasons[:5]


def build_match_result(score: int, p: Dict[str, Any]) -> Dict[str, Any]:
    reasons = describe_match_reasons("", p)
    return {
        "score": int(score),
        "profile": {
            "gmid": p["gmid"],
            "alias_name": (p.get("alias_name") or "").strip(),
            "display_name": p["display_name"],
            "headline": p.get("headline") or "",
            "biography": p.get("biography") or "",
            "domains": p.get("domains") or [],
            "roles": p.get("roles") or [],
            "experience_years": p.get("experience_years") or 0,
            "assets_preview": (p.get("assets") or [])[:6],
            "networks": p.get("networks") or [],
            "values": p.get("values") or [],
            "is_system": p.get("is_system"),
            "strength_score": profile_strength_score(p)
        },
        "reasons": reasons,
        "dimension_scores": match_dimension_scores("", p),
    }


def make_match_payload(query: str, requester: str, limit: int = 10) -> Dict[str, Any]:
    blocked = set()
    if requester:
        conn = get_conn()
        try:
            blocked = get_blocked_gmids(conn, requester)
        finally:
            put_conn(conn)
    profiles = [
        p for p in fetch_profiles(5000, exclude_gmids=blocked)
        if p["gmid"] != requester
        and p.get("status") in ("active",)
        and p.get("status") != "ghosted"
    ]
    scored = []
    for p in profiles:
        s = score_profile(query, p)
        if s > 0:
            scored.append((s, p))
    scored.sort(key=lambda x: (x[0], profile_strength_score(x[1])), reverse=True)
    out = []
    for s, p in scored[:limit]:
        out.append({
            "score": int(s),
            "profile": {
                "gmid": p["gmid"],
                "alias_name": (p.get("alias_name") or "").strip(),
                "display_name": p["display_name"],
                "headline": p.get("headline") or "",
                "biography": p.get("biography") or "",
                "domains": p.get("domains") or [],
                "roles": p.get("roles") or [],
                "experience_years": p.get("experience_years") or 0,
                "assets_preview": (p.get("assets") or [])[:6],
                "networks": p.get("networks") or [],
                "values": p.get("values") or [],
                "is_system": p.get("is_system"),
                "strength_score": profile_strength_score(p)
            },
            "reasons": describe_match_reasons(query, p),
            "dimension_scores": match_dimension_scores(query, p),
        })
    return {"ok": True, "query": query, "count": len(out), "results": out, "pool": "all_active_members_excluding_ghosts_and_blocks"}


def ai_shortlist_context(query: str, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    context = []
    for idx, item in enumerate(results[:8], start=1):
        profile = item.get("profile") or {}
        context.append({
            "rank": idx,
            "alias": (profile.get("alias_name") or "").strip(),
            "gmid": profile.get("gmid"),
            "score": item.get("score"),
            "headline": profile.get("headline") or "",
            "biography": profile.get("biography") or "",
            "roles": profile.get("roles") or [],
            "domains": profile.get("domains") or [],
            "networks": profile.get("networks") or [],
            "values": profile.get("values") or [],
            "experience_years": profile.get("experience_years") or 0,
            "strength_score": profile.get("strength_score") or 0,
            "assets_preview": profile.get("assets_preview") or [],
            "reasons": item.get("reasons") or [],
            "dimension_scores": item.get("dimension_scores") or {},
        })
    return context


def deterministic_ai_summary(query: str, shortlist: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not shortlist:
        return {
            "summary": "No shortlist is available yet. Run a match search first.",
            "top_candidates": [],
            "differences": [],
            "next_questions": ["What role do you need most?", "Which industry matters most?"],
            "source": "deterministic_fallback",
        }
    top = shortlist[:3]
    bullets = []
    top_candidates = []
    for person in top:
        role = ", ".join(person.get("roles")[:2]) or "Meridian member"
        domain = ", ".join(person.get("domains")[:3]) or "broad domain coverage"
        rationale = " ".join(person.get("reasons")[:3])
        bullets.append(f"{person['alias']} is a strong fit for {query.lower()} because of {role} experience across {domain}. {rationale}".strip())
        top_candidates.append({
            "alias": person["alias"],
            "gmid": person["gmid"],
            "score": person.get("score"),
            "rationale": rationale or f"Profile suggests fit via {domain}."
        })
    differences = []
    if len(top) >= 2:
        differences.append(f"{top[0]['alias']} appears strongest overall, while {top[1]['alias']} may be better if you prioritize {', '.join((top[1].get('domains') or ['domain specificity'])[:2])}.")
    if len(top) >= 3:
        differences.append(f"{top[2]['alias']} looks more differentiated around {', '.join((top[2].get('networks') or ['network access'])[:2])}.")
    next_questions = [
        "Do you care more about operator depth, investor reach, or board access?",
        "Should Meridian favor discretion, warm network path, or direct domain expertise?",
        "Which industry or geography should be treated as mandatory?",
    ]
    return {
        "summary": " ".join(bullets[:2]),
        "top_candidates": top_candidates,
        "differences": differences,
        "next_questions": next_questions,
        "source": "deterministic_fallback",
    }


def get_openai_client():
    if not OPENAI_API_KEY or OpenAI is None:
        return None
    kwargs = {"api_key": OPENAI_API_KEY}
    if OPENAI_PROJECT_ID:
        kwargs["project"] = OPENAI_PROJECT_ID
    try:
        return OpenAI(**kwargs)
    except Exception:
        return None


def ai_json_response(system_prompt: str, user_payload: Dict[str, Any]) -> Dict[str, Any]:
    client = get_openai_client()
    if client is None:
        raise HTTPException(status_code=503, detail="AI unavailable. Please retry.")
    try:
        response = client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": [{"type": "text", "text": system_prompt}]},
                {"role": "user", "content": [{"type": "text", "text": json.dumps(user_payload, ensure_ascii=False)}]},
            ],
            text={"format": {"type": "json_object"}},
        )
        raw = getattr(response, "output_text", None)
        if not raw:
            raise HTTPException(status_code=503, detail="AI unavailable. Please retry.")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise HTTPException(status_code=503, detail="AI unavailable. Please retry.")
        return data
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=503, detail="AI unavailable. Please retry.")


def generate_ai_match_summary(query: str, results: List[Dict[str, Any]]) -> Dict[str, Any]:
    shortlist = ai_shortlist_context(query, results)
    if not shortlist:
        raise HTTPException(status_code=400, detail="Run a search first.")
    system_prompt = """You are Meridian AI Concierge. You must stay grounded only in the shortlist profiles provided. Never invent facts, identities, relationships, or companies beyond the supplied data. Return JSON with keys: summary, top_candidates, differences, next_questions, confidence_note. top_candidates should be an array of objects with alias, gmid, score, rationale. differences and next_questions should be arrays of short strings. When comparing people, reference only aliases that appear in the shortlist payload."""
    user_payload = {"query": query, "shortlist": shortlist}
    data = ai_json_response(system_prompt, user_payload)
    data["source"] = "openai"
    data.setdefault("summary", "")
    data.setdefault("top_candidates", [])
    data.setdefault("differences", [])
    data.setdefault("next_questions", [])
    return data


def deterministic_ai_chat(query: str, shortlist: List[Dict[str, Any]], question: str) -> Dict[str, Any]:
    if not shortlist:
        return {"answer": "Run a match search first so Meridian can narrow the shortlist before answering follow-up questions.", "suggested_focus": []}

    q = (question or "").strip().lower()
    suggested_focus = [x['alias'] for x in shortlist[:3]]
    if not q:
        return {
            "answer": "Ask Meridian AI to compare the shortlist by operator depth, investor reach, discretion, network warmth, or domain fit.",
            "suggested_focus": suggested_focus,
            "source": "deterministic_fallback"
        }

    if re.search(r"\bhow old\b|\bage\b", q):
        years = [f"{p['alias']} ({int(p.get('experience_years') or 0)} years experience)" for p in shortlist[:3]]
        return {
            "answer": "Meridian does not infer exact ages from the shortlist. It stays grounded in profile evidence such as years of experience. Top examples here are " + ", ".join(years) + ".",
            "suggested_focus": suggested_focus,
            "source": "deterministic_fallback"
        }

    if re.fullmatch(r"(?:what is\s+)?[-+*/(). 0-9]+[?!. ]*", q):
        expr = re.sub(r"[^0-9+\-*/(). ]", "", q).strip()
        result = None
        if expr:
            try:
                result = eval(expr, {"__builtins__": {}}, {})
            except Exception:
                result = None
        if result is not None:
            return {
                "answer": f"That question is outside shortlist narrowing, but the result is {result}. For Meridian, ask which profile is strongest for operator depth, investor reach, board access, or discretion.",
                "suggested_focus": suggested_focus,
                "source": "deterministic_fallback"
            }
        return {
            "answer": "That question is outside shortlist narrowing. Ask Meridian AI to compare the candidates by fit, trust, domain, or warm-path potential.",
            "suggested_focus": suggested_focus,
            "source": "deterministic_fallback"
        }

    shortlist_terms = set(tokenize(query))
    for person in shortlist[:5]:
        shortlist_terms.update(tokenize(person.get("alias") or ""))
        shortlist_terms.update(tokenize(" ".join(person.get("roles") or [])))
        shortlist_terms.update(tokenize(" ".join(person.get("domains") or [])))
        shortlist_terms.update(tokenize(" ".join(person.get("networks") or [])))

    q_tokens = set(tokenize(q))
    if q_tokens and not (q_tokens & shortlist_terms):
        return {
            "answer": "That question does not appear to narrow the current shortlist yet. Ask about investor reach, operator depth, discretion, domain overlap, board access, or which profile is the safest introduction path.",
            "suggested_focus": suggested_focus,
            "source": "deterministic_fallback"
        }

    ranked = shortlist
    if "discreet" in q or "discretion" in q or "confidential" in q:
        ranked = sorted(shortlist, key=lambda x: sum(1 for r in x.get("reasons", []) if any(k in r.lower() for k in ["warm path", "network overlap", "value signal"])), reverse=True)
    elif "network" in q or "warm" in q or "intro" in q or "relationship" in q:
        ranked = sorted(shortlist, key=lambda x: len(x.get("networks") or []), reverse=True)
    elif "operator" in q or "operat" in q or "execution" in q:
        ranked = sorted(shortlist, key=lambda x: len([r for r in x.get("roles") or [] if any(tok in r.lower() for tok in ["coo","director","lead","operations","program","manufacturing","procurement","supply"])]), reverse=True)
    elif "investor" in q or "private equity" in q or "capital" in q or "board" in q:
        ranked = sorted(shortlist, key=lambda x: len([n for n in x.get("networks") or [] if any(tok in n.lower() for tok in ["investor","lp","board","pe","operating"])]), reverse=True)
    elif "domain" in q or "industry" in q or "sector" in q:
        ranked = sorted(shortlist, key=lambda x: len(x.get("domains") or []), reverse=True)

    best = ranked[0]
    answer = f"Based on this follow-up, {best['alias']} looks strongest because of {', '.join((best.get('roles') or ['profile fit'])[:2])}, domain coverage in {', '.join((best.get('domains') or ['relevant areas'])[:3])}, and signals such as {' '.join((best.get('reasons') or [])[:2])}."
    return {"answer": answer, "suggested_focus": [x['alias'] for x in ranked[:3]], "source": "deterministic_fallback"}

def generate_ai_match_chat(query: str, results: List[Dict[str, Any]], messages: List[Dict[str, str]]) -> Dict[str, Any]:
    shortlist = ai_shortlist_context(query, results)
    if not shortlist:
        raise HTTPException(status_code=400, detail="Run a search first.")
    if not messages:
        raise HTTPException(status_code=400, detail="Question required")
    system_prompt = """You are Meridian AI Concierge. Answer only from the provided shortlist. Do not mention people outside the shortlist. Do not invent facts. Be concise, high-signal, and comparison-oriented. Return JSON with keys: answer, suggested_focus, confidence_note. For comparisons, use only aliases present in the shortlist payload."""
    user_payload = {"query": query, "shortlist": shortlist, "messages": messages[-8:]}
    data = ai_json_response(system_prompt, user_payload)
    data["source"] = "openai"
    data.setdefault("answer", "")
    data.setdefault("suggested_focus", [])
    return data

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
              alias_name TEXT UNIQUE,
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
            CREATE TABLE IF NOT EXISTS member_ghost_snapshots (
              member_id BIGINT PRIMARY KEY REFERENCES members(id) ON DELETE CASCADE,
              display_name TEXT,
              email TEXT,
              previous_status TEXT,
              previous_is_system BOOLEAN NOT NULL DEFAULT FALSE,
              auth_username TEXT,
              auth_password_hash TEXT,
              auth_must_change_password BOOLEAN,
              profile_headline TEXT,
              profile_biography TEXT,
              profile_domains_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              profile_roles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              profile_experience_years INT NOT NULL DEFAULT 0,
              profile_networks_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              profile_political_social_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              profile_assets_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              profile_values_json JSONB NOT NULL DEFAULT '[]'::jsonb,
              profile_attributes_json JSONB NOT NULL DEFAULT '{}'::jsonb,
              profile_strength_score INT NOT NULL DEFAULT 0,
              ghosted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
              responded_at TIMESTAMPTZ,
              recipient_seen_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS chat_messages (
              id BIGSERIAL PRIMARY KEY,
              ping_id BIGINT NOT NULL REFERENCES pings(id) ON DELETE CASCADE,
              sender_gmid TEXT NOT NULL,
              message TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS member_blocks (
              id BIGSERIAL PRIMARY KEY,
              blocker_gmid TEXT NOT NULL,
              blocked_gmid TEXT NOT NULL,
              created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
              UNIQUE(blocker_gmid, blocked_gmid)
            );
            """)
            cur.execute("ALTER TABLE members ADD COLUMN IF NOT EXISTS alias_name TEXT")
            cur.execute("ALTER TABLE member_documents ADD COLUMN IF NOT EXISTS source_type TEXT")
            cur.execute("ALTER TABLE pings ADD COLUMN IF NOT EXISTS recipient_seen_at TIMESTAMPTZ")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_members_alias_name_unique ON members(alias_name) WHERE alias_name IS NOT NULL")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_member_blocks_blocker ON member_blocks(blocker_gmid)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_member_blocks_blocked ON member_blocks(blocked_gmid)")
            cur.execute("UPDATE member_documents SET source_type=COALESCE(source_type, content_type, 'upload') WHERE source_type IS NULL")
            cur.execute("SELECT COUNT(*) FROM members")
            seed_demo_members = (cur.fetchone()[0] == 0)
            cur.execute("SELECT id, gmid FROM members WHERE display_name=%s", ("Mike S",))
            mike_row = cur.fetchone()
            if not mike_row:
                gmid = make_gmid("Mike S|PRINCIPAL")
                cur.execute("INSERT INTO members (gmid, display_name, email, alias_name, is_system, status) VALUES (%s,%s,%s,%s,%s,%s) RETURNING id, gmid", (gmid, "Mike S", "mike@meridian.local", canonical_alias(cur, gmid), False, "active"))
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
            if seed_demo_members:
                first = ["Avery","Jordan","Riley","Casey","Morgan","Taylor","Quinn","Hayden","Parker","Rowan","Blake","Cameron","Drew","Emerson","Finley","Harper","Kai","Logan","Micah","Noel"]
                last  = ["Stone","Reed","Carter","Hayes","Brooks","Wells","Foster","Shaw","Bennett","Cole","Sullivan","Pierce","Vaughn","Donovan","Holland","Walsh","Hayward","Monroe","Kendall","Navarro"]
                for i in range(100):
                    rnd = random.Random(i + 77)
                    display = f"{first[i % len(first)]} {last[(i*3) % len(last)]} — EX-{i+1:03d}"
                    gmid = make_gmid("SYSTEM|" + display)
                    cur.execute("INSERT INTO members (gmid, display_name, alias_name, is_system, status) VALUES (%s,%s,%s,%s,%s) RETURNING id", (gmid, display, canonical_alias(cur, gmid), True, "active"))
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
            cur.execute("SELECT id FROM members WHERE lower(coalesce(email,''))='darrin.joncas@gmail.com' OR lower(display_name)='darrin joncas' ORDER BY id ASC LIMIT 1")
            darrin_row = cur.fetchone()
            if darrin_row:
                darrin_id = darrin_row[0]
                cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s)", ("darrin.joncas",))
                owner = cur.fetchone()
                if owner and owner[0] != darrin_id:
                    cur.execute("UPDATE member_auth SET username=%s WHERE member_id=%s", (f"member.{owner[0]}", owner[0]))
                cur.execute("SELECT 1 FROM member_auth WHERE member_id=%s", (darrin_id,))
                if cur.fetchone():
                    cur.execute("UPDATE member_auth SET username=%s, password_hash=%s, must_change_password=FALSE WHERE member_id=%s", ("darrin.joncas", hash_password("red123"), darrin_id))
                else:
                    cur.execute("INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)", (darrin_id, "darrin.joncas", hash_password("red123"), False))
            cur.execute("SELECT id, gmid FROM members WHERE alias_name IS NULL OR alias_name='' ORDER BY id ASC")
            for alias_row in cur.fetchall():
                cur.execute("UPDATE members SET alias_name=%s WHERE id=%s", (canonical_alias(cur, alias_row[1], alias_row[0]), alias_row[0]))
            cur.execute("""INSERT INTO member_profiles (member_id)
                           SELECT m.id FROM members m
                           LEFT JOIN member_profiles p ON p.member_id = m.id
                           WHERE p.member_id IS NULL""")
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
                if (email and email.lower() == "darrin.joncas@gmail.com") or (display_name or "").strip().lower() == "darrin joncas" or (existing_username or "").strip().lower() == "darrin.joncas":
                    desired_username = "darrin.joncas"
                    desired_password = "red123"
                    must_change = False
                else:
                    desired_username = demo_login_username_for_gmid(row_gmid) if is_system else (email.split("@")[0] if email else demo_username_for_member(display_name, False, member_id))
                    if is_system:
                        desired_password = "red123"
                        must_change = False
                    else:
                        desired_password = f"Meridian-{make_gmid((email or display_name or str(member_id)))[:8]}"
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

CANONICAL_VISIBLE_MEMBER_SQL = "COALESCE(m.status, 'active') <> 'ghost'"

@app.on_event("startup")
def startup(): init_schema()

def ensure_member_blocks_table(conn):
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS member_blocks (
                          id BIGSERIAL PRIMARY KEY,
                          blocker_gmid TEXT NOT NULL,
                          blocked_gmid TEXT NOT NULL,
                          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                          UNIQUE(blocker_gmid, blocked_gmid)
                       );""")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_member_blocks_blocker ON member_blocks(blocker_gmid)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_member_blocks_blocked ON member_blocks(blocked_gmid)")
    conn.commit()

def ensure_canonical_member_rows(conn):
    with conn.cursor() as cur:
        # Meridian no longer holds invited members in a conditional pending state.
        # Any invited member is promoted directly into the active canonical pool.
        cur.execute("UPDATE members SET status='active' WHERE status='pending_vetting'")

        cur.execute(f"SELECT id, gmid FROM members m WHERE {CANONICAL_VISIBLE_MEMBER_SQL} AND (m.alias_name IS NULL OR m.alias_name='') ORDER BY m.id ASC")
        for member_id, gmid in cur.fetchall():
            cur.execute("UPDATE members SET alias_name=%s WHERE id=%s", (canonical_alias(cur, gmid, member_id), member_id))

        cur.execute(f"""INSERT INTO member_profiles (member_id)
                       SELECT m.id FROM members m
                       LEFT JOIN member_profiles p ON p.member_id=m.id
                       WHERE {CANONICAL_VISIBLE_MEMBER_SQL} AND p.member_id IS NULL""")
    conn.commit()

def get_blocked_gmids(conn, member_gmid: str) -> set[str]:
    if not member_gmid:
        return set()
    try:
        ensure_member_blocks_table(conn)
        with conn.cursor() as cur:
            cur.execute("""SELECT blocker_gmid, blocked_gmid
                           FROM member_blocks
                           WHERE blocker_gmid=%s OR blocked_gmid=%s""", (member_gmid, member_gmid))
            blocked = set()
            for blocker_gmid, blocked_gmid in cur.fetchall():
                if blocker_gmid == member_gmid:
                    blocked.add(blocked_gmid)
                elif blocked_gmid == member_gmid:
                    blocked.add(blocker_gmid)
            return blocked
    except Exception:
        conn.rollback()
        return set()

def is_blocked_pair(conn, member_a: str, member_b: str) -> bool:
    if not member_a or not member_b or member_a == member_b:
        return False
    try:
        ensure_member_blocks_table(conn)
        with conn.cursor() as cur:
            cur.execute("""SELECT 1
                           FROM member_blocks
                           WHERE (blocker_gmid=%s AND blocked_gmid=%s)
                              OR (blocker_gmid=%s AND blocked_gmid=%s)
                           LIMIT 1""", (member_a, member_b, member_b, member_a))
            return cur.fetchone() is not None
    except Exception:
        conn.rollback()
        return False

def fetch_profiles(limit: int = 250, exclude_gmids=None):
    conn = get_conn()
    try:
        ensure_canonical_member_rows(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""SELECT m.id, m.gmid, m.alias_name, m.display_name, m.email, m.is_system, m.status, m.created_at,
                                  COALESCE(p.headline, '') AS headline,
                                  COALESCE(p.biography, '') AS biography,
                                  COALESCE(p.domains_json, '[]'::jsonb) AS domains,
                                  COALESCE(p.roles_json, '[]'::jsonb) AS roles,
                                  COALESCE(p.experience_years, 0) AS experience_years,
                                  COALESCE(p.networks_json, '[]'::jsonb) AS networks,
                                  COALESCE(p.political_social_json, '[]'::jsonb) AS political_social,
                                  COALESCE(p.assets_json, '[]'::jsonb) AS assets,
                                  COALESCE(p.values_json, '[]'::jsonb) AS values,
                                  COALESCE(p.attributes_json, '{{}}'::jsonb) AS attributes,
                                  COALESCE(p.strength_score, 0) AS strength_score,
                                  p.updated_at
                           FROM members m
                           LEFT JOIN member_profiles p ON p.member_id = m.id
                           WHERE {CANONICAL_VISIBLE_MEMBER_SQL}
                             AND COALESCE(m.alias_name, '') <> ''
                           ORDER BY m.is_system DESC, COALESCE(p.strength_score,0) DESC, COALESCE(p.experience_years,0) DESC, m.created_at DESC, m.id ASC
                           LIMIT %s""", (limit,))
            rows = cur.fetchall()
            excluded = set(exclude_gmids or [])
            if excluded:
                rows = [row for row in rows if row.get('gmid') not in excluded]
            return rows
    finally:
        put_conn(conn)

def canonical_visible_member_count() -> int:
    conn = get_conn()
    try:
        ensure_canonical_member_rows(conn)
        with conn.cursor() as cur:
            cur.execute(f"""SELECT COUNT(*)
                           FROM members m
                           WHERE {CANONICAL_VISIBLE_MEMBER_SQL} AND COALESCE(m.alias_name, '') <> ''""")
            return int(cur.fetchone()[0])
    finally:
        put_conn(conn)

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
        ensure_canonical_member_rows(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT gmid, display_name, email, status, is_system FROM members WHERE gmid=%s AND status IN ('active')", (gmid,))
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
            if not row or row["status"] not in ("active",) or not verify_password(password, row["password_hash"]):
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

@app.get("/api/profiles/self")
def api_profile_self(request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, gmid, display_name, email, status, is_system, alias_name FROM members WHERE gmid=%s", (member["gmid"],))
            member_row = cur.fetchone()
            if not member_row:
                raise HTTPException(status_code=404, detail="member not found")
            member_id = member_row["id"]

            cur.execute("INSERT INTO member_profiles (member_id) VALUES (%s) ON CONFLICT (member_id) DO NOTHING", (member_id,))
            conn.commit()

            cur.execute("""SELECT
                              headline,
                              biography,
                              domains_json AS domains,
                              roles_json AS roles,
                              experience_years,
                              networks_json AS networks,
                              political_social_json AS political_social,
                              assets_json AS assets,
                              values_json AS values,
                              attributes_json AS attributes,
                              strength_score,
                              updated_at
                           FROM member_profiles
                           WHERE member_id=%s""", (member_id,))
            profile_row = cur.fetchone() or {}

            profile = {
                "gmid": member_row["gmid"],
                "alias_name": member_row.get("alias_name"),
                "display_name": member_row.get("display_name"),
                "email": member_row.get("email"),
                "status": member_row.get("status"),
                "is_system": member_row.get("is_system"),
                "headline": profile_row.get("headline"),
                "biography": profile_row.get("biography"),
                "domains": profile_row.get("domains") or [],
                "roles": profile_row.get("roles") or [],
                "experience_years": profile_row.get("experience_years") or 0,
                "networks": profile_row.get("networks") or [],
                "political_social": profile_row.get("political_social") or [],
                "assets": profile_row.get("assets") or [],
                "values": profile_row.get("values") or [],
                "attributes": profile_row.get("attributes") or {},
                "strength_score": profile_row.get("strength_score") or 0,
                "updated_at": profile_row.get("updated_at"),
            }

            docs = []
            try:
                cur.execute("""SELECT id, filename, COALESCE(source_type, content_type, 'upload') AS source_type, parsed_json, uploaded_at
                               FROM member_documents
                               WHERE member_id=%s
                               ORDER BY uploaded_at DESC
                               LIMIT 10""", (member_id,))
                docs = cur.fetchall() or []
            except Exception:
                conn.rollback()
                docs = []

            return JSONResponse(content=jsonable_encoder({"ok": True, "profile": profile, "documents": docs, "alias": member_row.get("alias_name") or ""}))
    finally:
        put_conn(conn)

@app.get("/api/profile/me")
def api_profile_me_legacy(request: Request):
    return api_profile_self(request)

@app.get("/api/profiles/{gmid}")
def api_profile_public(gmid: str):
    if gmid == "me":
        raise HTTPException(status_code=400, detail="use /api/profiles/self")
    current = next((p for p in fetch_profiles(5000) if p["gmid"] == gmid), None)
    if not current:
        raise HTTPException(status_code=404, detail="profile not found")
    return JSONResponse(content=jsonable_encoder({"ok": True, "profile": current}))

@app.get("/api/profile/{gmid}")
def api_profile(gmid: str):
    return api_profile_public(gmid)

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
            cur.execute("INSERT INTO member_profiles (member_id) VALUES (%s) ON CONFLICT (member_id) DO NOTHING", (member_id,))
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
            cur.execute("INSERT INTO members (gmid, display_name, email, alias_name, is_system, status) VALUES (%s,%s,%s,%s,FALSE,'active') RETURNING id", (gmid, display_name, email, canonical_alias(cur, gmid)))
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
    data = await request.json()
    q = (data.get("query") or "").strip()
    requester = (data.get("requester_gmid") or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="query is required")
    return JSONResponse(content=jsonable_encoder(make_match_payload(q, requester, 10)))

@app.post("/api/ai/match-summary")
async def api_ai_match_summary(request: Request):
    data = await request.json()
    q = (data.get("query") or "").strip()
    requester = (data.get("requester_gmid") or "").strip()
    results = data.get("results") or []
    if not q:
        raise HTTPException(status_code=400, detail="query is required")
    if not results:
        results = make_match_payload(q, requester, 8)["results"]
    summary = generate_ai_match_summary(q, results)
    return JSONResponse(content=jsonable_encoder({"ok": True, "query": q, "summary": summary, "results_used": len(results)}))

@app.post("/api/ai/match-chat")
async def api_ai_match_chat(request: Request):
    data = await request.json()
    q = (data.get("query") or "").strip()
    requester = (data.get("requester_gmid") or "").strip()
    results = data.get("results") or []
    messages = data.get("messages") or []
    if not q:
        raise HTTPException(status_code=400, detail="query is required")
    if not results:
        results = make_match_payload(q, requester, 8)["results"]
    answer = generate_ai_match_chat(q, results, messages)
    return JSONResponse(content=jsonable_encoder({"ok": True, "query": q, "answer": answer}))

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
            if is_blocked_pair(conn, requester, target):
                raise HTTPException(status_code=403, detail="This member connection is blocked")
            cur.execute("INSERT INTO pings (requester_gmid, target_gmid, request_text, score, status) VALUES (%s,%s,%s,%s,'pending') RETURNING id", (requester, target, txt, score))
            return JSONResponse(content=jsonable_encoder({"ok": True, "ping_id": cur.fetchone()["id"]}))
    finally: put_conn(conn)

@app.get("/api/blocks")
def api_blocks(request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_conn()
    try:
        ensure_member_blocks_table(conn)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT b.blocked_gmid AS gmid, COALESCE(m.alias_name, b.blocked_gmid) AS alias_name, b.created_at
                           FROM member_blocks b
                           LEFT JOIN members m ON m.gmid=b.blocked_gmid
                           WHERE b.blocker_gmid=%s
                           ORDER BY b.created_at DESC""", (member["gmid"],))
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": cur.fetchall()}))
    except Exception:
        conn.rollback()
        return JSONResponse(content=jsonable_encoder({"ok": True, "items": []}))
    finally:
        put_conn(conn)

@app.post("/api/blocks/{blocked_gmid}")
def api_block_member(blocked_gmid: str, request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    blocker_gmid = member["gmid"]
    blocked_gmid = (blocked_gmid or "").strip()
    if not blocked_gmid or blocked_gmid == blocker_gmid:
        raise HTTPException(status_code=400, detail="Invalid block target")
    conn = get_conn()
    try:
        ensure_member_blocks_table(conn)
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT gmid, alias_name, status FROM members WHERE gmid=%s", (blocked_gmid,))
            target = cur.fetchone()
            if not target or target["status"] != "active":
                raise HTTPException(status_code=404, detail="Member not found")
            cur.execute("""INSERT INTO member_blocks (blocker_gmid, blocked_gmid)
                           VALUES (%s,%s)
                           ON CONFLICT (blocker_gmid, blocked_gmid) DO NOTHING""", (blocker_gmid, blocked_gmid))
            cur.execute("DELETE FROM pings WHERE (requester_gmid=%s AND target_gmid=%s) OR (requester_gmid=%s AND target_gmid=%s)",
                        (blocker_gmid, blocked_gmid, blocked_gmid, blocker_gmid))
            return JSONResponse(content=jsonable_encoder({"ok": True, "blocked_gmid": blocked_gmid, "alias_name": target.get("alias_name") or ""}))
    finally:
        put_conn(conn)

@app.delete("/api/blocks/{blocked_gmid}")
def api_unblock_member(blocked_gmid: str, request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_conn()
    try:
        ensure_member_blocks_table(conn)
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM member_blocks WHERE blocker_gmid=%s AND blocked_gmid=%s", (member["gmid"], blocked_gmid))
            deleted = cur.rowcount
            return JSONResponse(content=jsonable_encoder({"ok": True, "removed": bool(deleted), "blocked_gmid": blocked_gmid}))
    finally:
        put_conn(conn)

@app.get("/api/inbox/{gmid}")
def api_inbox(gmid: str, limit: int = 200):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.*,
                                  req.alias_name AS requester_alias,
                                  tgt.alias_name AS target_alias,
                                  CASE WHEN p.recipient_seen_at IS NULL THEN FALSE ELSE TRUE END AS recipient_has_seen
                           FROM pings p
                           LEFT JOIN members req ON req.gmid=p.requester_gmid
                           LEFT JOIN members tgt ON tgt.gmid=p.target_gmid
                           WHERE p.target_gmid=%s
                             AND NOT EXISTS (
                                 SELECT 1 FROM member_blocks b
                                 WHERE (b.blocker_gmid=%s AND b.blocked_gmid IN (p.requester_gmid, p.target_gmid))
                                    OR (b.blocked_gmid=%s AND b.blocker_gmid IN (p.requester_gmid, p.target_gmid))
                             )
                           ORDER BY p.created_at DESC LIMIT %s""", (gmid, gmid, gmid, limit))
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": cur.fetchall()}))
    finally: put_conn(conn)

@app.get("/api/outbox/{gmid}")
def api_outbox(gmid: str, limit: int = 200):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.*,
                                  req.alias_name AS requester_alias,
                                  tgt.alias_name AS target_alias,
                                  CASE WHEN p.recipient_seen_at IS NULL THEN FALSE ELSE TRUE END AS recipient_has_seen
                           FROM pings p
                           LEFT JOIN members req ON req.gmid=p.requester_gmid
                           LEFT JOIN members tgt ON tgt.gmid=p.target_gmid
                           WHERE p.requester_gmid=%s
                             AND NOT EXISTS (
                                 SELECT 1 FROM member_blocks b
                                 WHERE (b.blocker_gmid=%s AND b.blocked_gmid IN (p.requester_gmid, p.target_gmid))
                                    OR (b.blocked_gmid=%s AND b.blocker_gmid IN (p.requester_gmid, p.target_gmid))
                             )
                           ORDER BY p.created_at DESC LIMIT %s""", (gmid, gmid, gmid, limit))
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": cur.fetchall()}))
    finally: put_conn(conn)

@app.post("/api/ping/{ping_id}/read")
def api_mark_ping_read(ping_id: int, request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""UPDATE pings
                           SET recipient_seen_at=COALESCE(recipient_seen_at, NOW())
                           WHERE id=%s AND target_gmid=%s
                           RETURNING id, recipient_seen_at""", (ping_id, member["gmid"]))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Ping not found")
            return JSONResponse(content=jsonable_encoder({"ok": True, "ping_id": row["id"], "recipient_seen_at": row["recipient_seen_at"]}))
    finally:
        put_conn(conn)

@app.post("/api/ping/{ping_id}/respond")
async def api_respond(ping_id: int, request: Request):
    member = get_current_member(request)
    if not member:
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = await request.json(); status = (data.get("status") or "").strip().lower()
    if status not in ("accepted","declined"): raise HTTPException(status_code=400, detail="status must be accepted or declined")
    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""UPDATE pings
                           SET status=%s, responded_at=NOW(), recipient_seen_at=COALESCE(recipient_seen_at, NOW())
                           WHERE id=%s AND target_gmid=%s""", (status, ping_id, member["gmid"]))
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="Ping not found")
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
        blocked = get_blocked_gmids(conn, gmid)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT p.id, p.requester_gmid, p.target_gmid, p.created_at, p.responded_at,
                                  req.alias_name AS requester_alias,
                                  tgt.alias_name AS target_alias
                           FROM pings p
                           LEFT JOIN members req ON req.gmid=p.requester_gmid
                           LEFT JOIN members tgt ON tgt.gmid=p.target_gmid
                           WHERE p.status='accepted'
                             AND (p.requester_gmid=%s OR p.target_gmid=%s)
                             AND NOT EXISTS (
                                 SELECT 1 FROM member_blocks b
                                 WHERE (b.blocker_gmid=%s AND b.blocked_gmid IN (p.requester_gmid, p.target_gmid))
                                    OR (b.blocked_gmid=%s AND b.blocker_gmid IN (p.requester_gmid, p.target_gmid))
                             )
                           ORDER BY COALESCE(p.responded_at, p.created_at) DESC, p.id DESC""", (gmid, gmid, gmid, gmid))
            rows = cur.fetchall()
            nodes, edges, seen, seen_others = [], [], set(), set()
            def add_node(node_gmid, label):
                if node_gmid not in seen:
                    seen.add(node_gmid)
                    nodes.append({"gmid": node_gmid, "label": label})
            add_node(gmid, "Me")
            direct_gmids = []
            alias_map = {gmid: "Me"}
            for r in rows:
                other = r["target_gmid"] if r["requester_gmid"] == gmid else r["requester_gmid"]
                other_alias = (r["target_alias"] if r["requester_gmid"] == gmid else r["requester_alias"]) or other
                alias_map[other] = other_alias
                add_node(other, other_alias)
                if other not in direct_gmids:
                    direct_gmids.append(other)
                if other in seen_others:
                    continue
                seen_others.add(other)
                edges.append({"ping_id": r["id"], "other_gmid": other, "other_name": other_alias, "created_at": r["created_at"], "responded_at": r["responded_at"]})

            second_degree_nodes = []
            dotted_edges = []
            recommendations = []
            if direct_gmids:
                placeholders = ",".join(["%s"] * len(direct_gmids))
                params = tuple(direct_gmids) + tuple(direct_gmids)
                cur.execute(f"""SELECT p.requester_gmid, p.target_gmid,
                                       req.alias_name AS requester_alias,
                                       tgt.alias_name AS target_alias
                                FROM pings p
                                LEFT JOIN members req ON req.gmid=p.requester_gmid
                                LEFT JOIN members tgt ON tgt.gmid=p.target_gmid
                                WHERE p.status='accepted'
                                  AND (p.requester_gmid IN ({placeholders}) OR p.target_gmid IN ({placeholders}))""", params)
                linked = cur.fetchall()
                second_map = {}
                direct_set = set(direct_gmids)
                for r in linked:
                    a = r["requester_gmid"]
                    b = r["target_gmid"]
                    if a in direct_set and b != gmid:
                        via, candidate = a, b
                        candidate_alias = r["target_alias"] or b
                    elif b in direct_set and a != gmid:
                        via, candidate = b, a
                        candidate_alias = r["requester_alias"] or a
                    else:
                        continue
                    if candidate == gmid or candidate in direct_set or candidate in blocked or via in blocked:
                        continue
                    entry = second_map.setdefault(candidate, {
                        "gmid": candidate,
                        "label": candidate_alias,
                        "via_gmids": [],
                        "via_aliases": []
                    })
                    if via not in entry["via_gmids"]:
                        entry["via_gmids"].append(via)
                        entry["via_aliases"].append(alias_map.get(via, via))
                second_degree_nodes = sorted([
                    {
                        "gmid": v["gmid"],
                        "label": v["label"],
                        "via_gmids": v["via_gmids"],
                        "via_aliases": v["via_aliases"],
                        "mutual_count": len(v["via_gmids"])
                    }
                    for v in second_map.values()
                ], key=lambda x: (-x["mutual_count"], x["label"].lower()))
                for node in second_degree_nodes:
                    for via in node["via_gmids"]:
                        dotted_edges.append({"from_gmid": via, "to_gmid": node["gmid"]})
                recommendations = second_degree_nodes[:8]
            return JSONResponse(content=jsonable_encoder({
                "ok": True,
                "center_gmid": gmid,
                "nodes": nodes,
                "edges": edges,
                "second_degree_nodes": second_degree_nodes,
                "dotted_edges": dotted_edges,
                "recommendations": recommendations
            }))
    finally: put_conn(conn)

@app.get("/api/rankings")
def api_rankings(request: Request, limit: int = 500):
    member = get_current_member(request)
    blocked = set()
    if member:
        conn_for_blocks = get_conn()
        try:
            blocked = get_blocked_gmids(conn_for_blocks, member["gmid"])
        finally:
            put_conn(conn_for_blocks)
    profiles = fetch_profiles(5000, exclude_gmids=blocked)
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            out=[]
            for p in profiles:
                if p.get("status") not in ("active",):
                    continue
                cur.execute("SELECT COUNT(*) AS c FROM pings WHERE status='accepted' AND (requester_gmid=%s OR target_gmid=%s)", (p["gmid"], p["gmid"]))
                connections = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM pings WHERE requester_gmid=%s", (p["gmid"],)); sent = cur.fetchone()["c"]
                cur.execute("SELECT COUNT(*) AS c FROM pings WHERE requester_gmid=%s AND status='accepted'", (p["gmid"],)); accepted_sent = cur.fetchone()["c"]
                response_rate = (accepted_sent / sent) if sent else 0
                strength = profile_strength_score(p)
                composite = round((strength * 0.72) + (min(connections,10) * 2.0) + (response_rate * 8.0), 2)
                out.append({"gmid": p["gmid"], "alias_name": (p.get("alias_name") or "").strip(), "display_name": p["display_name"], "strength_score": strength, "connections": connections, "response_rate": round(response_rate,2), "composite_score": composite, "domains": p["domains"]})
            out.sort(key=lambda x:(x["composite_score"], x["strength_score"], x["connections"]), reverse=True)
            for idx,row in enumerate(out, start=1): row["rank"]=idx
            return JSONResponse(content=jsonable_encoder({"ok": True, "items": out[:limit], "total": len(out), "canonical_visible_members": canonical_visible_member_count()}))
    finally: put_conn(conn)

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
            cur.execute("INSERT INTO members (gmid, display_name, email, alias_name, is_system, status) VALUES (%s,%s,%s,%s,FALSE,'active') RETURNING id", (gmid, display_name, email, canonical_alias(cur, gmid)))
            member_id = cur.fetchone()["id"]
            profile = {"domains":domains,"roles":roles,"experience_years":experience_years,"networks":networks,"assets":assets,"values":values,"attributes":attributes}
            username = unique_username(cur, username or email.split("@")[0])
            password = password or f"Meridian-{gmid[:8]}"
            cur.execute("INSERT INTO member_profiles (member_id, domains_json, roles_json, experience_years, networks_json, assets_json, values_json, attributes_json, strength_score) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", (member_id, Json(domains), Json(roles), experience_years, Json(networks), Json(assets), Json(values), Json(attributes), profile_strength_score(profile)))
            cur.execute("UPDATE member_invitations SET invitation_status='accepted', accepted_at=NOW() WHERE id=%s", (inv["id"],))
            cur.execute("INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)", (member_id, username, hash_password(password), False))
        return JSONResponse(content=jsonable_encoder({"ok": True, "gmid": gmid, "status": "active", "username": username}))
    finally: put_conn(conn)

@app.get("/api/members/discover")
def api_member_discovery(request: Request, limit: int = 120):
    member = get_current_member(request)
    blocked = set()
    if member:
        conn = get_conn()
        try:
            blocked = get_blocked_gmids(conn, member["gmid"])
        finally:
            put_conn(conn)
    profiles = fetch_profiles(5000, exclude_gmids=blocked)
    items = []
    for p in profiles:
        if p.get("status") not in ("active",):
            continue
        items.append({
            "gmid": p["gmid"],
            "alias": (p.get("alias_name") or "").strip(),
            "headline": ", ".join((p.get("roles") or [])[:2]) or "Meridian Member",
            "domains": p.get("domains") or [],
            "roles": p.get("roles") or [],
            "networks": p.get("networks") or [],
            "experience_years": p.get("experience_years") or 0,
            "strength_score": p.get("strength_score") or profile_strength_score(p)
        })
    items.sort(key=lambda x: (x["strength_score"], x["experience_years"]), reverse=True)
    return JSONResponse(content=jsonable_encoder({"ok": True, "items": items[:limit]}))

@app.get("/api/admin/members")
def api_admin_members(request: Request, limit: int = 500):
    if not get_current_admin(request):
        raise HTTPException(status_code=401, detail="Admin authentication required")
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT m.id, m.gmid, m.alias_name, m.display_name, m.email, m.is_system, m.status, a.username, a.must_change_password, a.last_login_at, m.created_at
                           FROM members m
                           LEFT JOIN member_auth a ON a.member_id = m.id
                           ORDER BY m.is_system DESC, m.created_at ASC, m.id ASC
                           LIMIT %s""", (limit,))
            rows = cur.fetchall()
            items = []
            for row in rows:
                alias_name = (row.get("alias_name") or "").strip()
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
            cur.execute("""SELECT m.id, m.gmid, m.display_name, m.email, m.status, m.is_system,
                                  a.username, a.password_hash, a.must_change_password,
                                  p.headline, p.biography, p.domains_json, p.roles_json, p.experience_years,
                                  p.networks_json, p.political_social_json, p.assets_json, p.values_json,
                                  p.attributes_json, p.strength_score
                           FROM members m
                           LEFT JOIN member_auth a ON a.member_id = m.id
                           LEFT JOIN member_profiles p ON p.member_id = m.id
                           WHERE m.id=%s""", (member_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Member not found")
            if row["status"] == "ghosted":
                return JSONResponse(content=jsonable_encoder({"ok": True, "ghosted": True, "member_id": member_id, "gmid": row["gmid"]}))

            cur.execute("""INSERT INTO member_ghost_snapshots (
                              member_id, display_name, email, previous_status, previous_is_system,
                              auth_username, auth_password_hash, auth_must_change_password,
                              profile_headline, profile_biography, profile_domains_json, profile_roles_json,
                              profile_experience_years, profile_networks_json, profile_political_social_json,
                              profile_assets_json, profile_values_json, profile_attributes_json, profile_strength_score
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (member_id) DO UPDATE SET
                              display_name=EXCLUDED.display_name,
                              email=EXCLUDED.email,
                              previous_status=EXCLUDED.previous_status,
                              previous_is_system=EXCLUDED.previous_is_system,
                              auth_username=EXCLUDED.auth_username,
                              auth_password_hash=EXCLUDED.auth_password_hash,
                              auth_must_change_password=EXCLUDED.auth_must_change_password,
                              profile_headline=EXCLUDED.profile_headline,
                              profile_biography=EXCLUDED.profile_biography,
                              profile_domains_json=EXCLUDED.profile_domains_json,
                              profile_roles_json=EXCLUDED.profile_roles_json,
                              profile_experience_years=EXCLUDED.profile_experience_years,
                              profile_networks_json=EXCLUDED.profile_networks_json,
                              profile_political_social_json=EXCLUDED.profile_political_social_json,
                              profile_assets_json=EXCLUDED.profile_assets_json,
                              profile_values_json=EXCLUDED.profile_values_json,
                              profile_attributes_json=EXCLUDED.profile_attributes_json,
                              profile_strength_score=EXCLUDED.profile_strength_score,
                              ghosted_at=NOW()""",
                        (member_id, row["display_name"], row["email"], row["status"], row["is_system"],
                         row["username"], row["password_hash"], row["must_change_password"],
                         row["headline"], row["biography"], Json(row.get("domains_json") or []), Json(row.get("roles_json") or []),
                         row.get("experience_years") or 0, Json(row.get("networks_json") or []), Json(row.get("political_social_json") or []),
                         Json(row.get("assets_json") or []), Json(row.get("values_json") or []), Json(row.get("attributes_json") or {}),
                         row.get("strength_score") or 0))
            cur.execute("DELETE FROM member_auth WHERE member_id=%s", (member_id,))
            cur.execute("""UPDATE member_profiles
                           SET headline=%s, biography=%s, domains_json='[]'::jsonb, roles_json='[]'::jsonb,
                               networks_json='[]'::jsonb, political_social_json='[]'::jsonb,
                               assets_json='[]'::jsonb, values_json='[]'::jsonb, attributes_json='{}'::jsonb,
                               strength_score=0, experience_years=0, updated_at=NOW()
                           WHERE member_id=%s""", ('GHOST MEMBER', 'Ghosted member record retained to preserve historical network links.', member_id))
            cur.execute("UPDATE members SET display_name=%s, email=NULL, status='ghosted', is_system=FALSE WHERE id=%s", ('GHOST MEMBER', member_id))
            return JSONResponse(content=jsonable_encoder({"ok": True, "ghosted": True, "member_id": member_id, "gmid": row["gmid"], "display_name": 'GHOST MEMBER'}))
    finally:
        put_conn(conn)

@app.post("/api/admin/members/{member_id}/unghost")
def api_admin_unghost_member(member_id: int, request: Request):
    if not get_current_admin(request):
        raise HTTPException(status_code=401, detail="Admin authentication required")
    conn = get_conn()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, gmid, alias_name, display_name, status FROM members WHERE id=%s", (member_id,))
            member = cur.fetchone()
            if not member:
                raise HTTPException(status_code=404, detail="Member not found")
            if member["status"] != "ghosted":
                return JSONResponse(content=jsonable_encoder({"ok": True, "restored": False, "member_id": member_id, "status": member["status"]}))

            cur.execute("SELECT * FROM member_ghost_snapshots WHERE member_id=%s", (member_id,))
            snap = cur.fetchone()
            restored_display = None
            restored_email = None
            restored_status = 'active'
            restored_is_system = False
            username = f"restored.{member_id}"
            password_hash = hash_password("red123")
            must_change_password = False
            profile = {
                'headline': None, 'biography': None, 'domains_json': [], 'roles_json': [], 'experience_years': 0,
                'networks_json': [], 'political_social_json': [], 'assets_json': [], 'values_json': [], 'attributes_json': {}, 'strength_score': 0
            }
            if snap:
                restored_display = snap.get('display_name') or f"Restored Member {member_id}"
                restored_email = snap.get('email')
                restored_status = snap.get('previous_status') or 'active'
                restored_is_system = bool(snap.get('previous_is_system'))
                username = snap.get('auth_username') or f"restored.{member_id}"
                password_hash = snap.get('auth_password_hash') or hash_password("red123")
                must_change_password = bool(snap.get('auth_must_change_password')) if snap.get('auth_must_change_password') is not None else False
                for key in profile:
                    if key in snap and snap.get(key) is not None:
                        profile[key] = snap.get(key)
            else:
                alias = member.get('alias_name') or f"member{member_id}"
                restored_display = alias
                profile['headline'] = 'Restored Meridian member'
                profile['biography'] = 'This member record was restored after being ghosted. Update the profile details as needed.'

            restored_status = restored_status if restored_status in ('active',) else 'active'
            cur.execute("UPDATE members SET display_name=%s, email=%s, status=%s, is_system=%s WHERE id=%s",
                        (restored_display, restored_email, restored_status, restored_is_system, member_id))
            cur.execute("""INSERT INTO member_profiles (
                              member_id, headline, biography, domains_json, roles_json, experience_years,
                              networks_json, political_social_json, assets_json, values_json, attributes_json, strength_score, updated_at
                           ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                           ON CONFLICT (member_id) DO UPDATE SET
                              headline=EXCLUDED.headline,
                              biography=EXCLUDED.biography,
                              domains_json=EXCLUDED.domains_json,
                              roles_json=EXCLUDED.roles_json,
                              experience_years=EXCLUDED.experience_years,
                              networks_json=EXCLUDED.networks_json,
                              political_social_json=EXCLUDED.political_social_json,
                              assets_json=EXCLUDED.assets_json,
                              values_json=EXCLUDED.values_json,
                              attributes_json=EXCLUDED.attributes_json,
                              strength_score=EXCLUDED.strength_score,
                              updated_at=NOW()""",
                        (member_id, profile['headline'], profile['biography'], Json(profile['domains_json'] or []), Json(profile['roles_json'] or []), profile['experience_years'] or 0,
                         Json(profile['networks_json'] or []), Json(profile['political_social_json'] or []), Json(profile['assets_json'] or []),
                         Json(profile['values_json'] or []), Json(profile['attributes_json'] or {}), profile['strength_score'] or 0))
            cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s) AND member_id<>%s", (username, member_id))
            owner = cur.fetchone()
            if owner:
                username = f"restored.{member_id}"
            cur.execute("""INSERT INTO member_auth (member_id, username, password_hash, must_change_password)
                           VALUES (%s,%s,%s,%s)
                           ON CONFLICT (member_id) DO UPDATE SET
                              username=EXCLUDED.username,
                              password_hash=EXCLUDED.password_hash,
                              must_change_password=EXCLUDED.must_change_password""",
                        (member_id, username, password_hash, must_change_password))
            return JSONResponse(content=jsonable_encoder({
                "ok": True,
                "restored": True,
                "member_id": member_id,
                "gmid": member["gmid"],
                "display_name": restored_display,
                "username": username,
                "status": restored_status,
                "temporary_password": "red123" if not snap or not snap.get('auth_password_hash') else None
            }))
    finally:
        put_conn(conn)

@app.get("/api/debug/db")
def api_debug():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) AS c FROM members")
            members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM members WHERE status IN ('active')")
            community_members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM members WHERE alias_name IS NOT NULL")
            alias_members = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM member_profiles")
            profile_rows = cur.fetchone()["c"]
            cur.execute("SELECT COUNT(*) AS c FROM members m JOIN member_profiles p ON p.member_id=m.id WHERE m.status IN ('active')")
            canonical_visible_members = cur.fetchone()["c"]
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
                "profile_rows": profile_rows,
                "alias_members": alias_members,
                "canonical_visible_members": canonical_visible_members,
                "system_members": system_members,
                "demo_members": system_members,
                "pings": pings,
                "accepted_pings": accepted_pings,
                "chat_messages": chats,
                "invitations": invites,
                "auth_accounts": auth_accounts
            }))
    finally: put_conn(conn)
