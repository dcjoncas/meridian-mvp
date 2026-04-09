#!/usr/bin/env python3
"""
github_candidate_miner.py

Single-file GitHub candidate sourcing script.

What it does:
- Searches public GitHub user profiles with REST search
- Enriches shortlisted users with GraphQL
- Scores candidates using public, self-declared, job-relevant signals
- Exports ranked results to CSV

Important:
- Use only public data users chose to publish
- Do not use for spam, bulk unsolicited outreach, or resale
- Requires a GitHub personal access token in GITHUB_TOKEN

Example:
    export GITHUB_TOKEN=ghp_xxx
    python github_candidate_miner.py --phrase "open to work" --location canada --keywords "python,fastapi,aws,ai" --output candidates.csv

Windows PowerShell:
    $env:GITHUB_TOKEN="ghp_xxx"
    python github_candidate_miner.py --phrase "open to work" --location canada --keywords "python,fastapi,aws,ai" --output candidates.csv

PowerShell note:
    If you use --query directly, wrap the whole query in double quotes:
    python github_candidate_miner.py --query "\"open to work\" in:bio location:canada"
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Iterable, List, Optional

try:
    import requests
except ImportError:
    print("Missing dependency: requests\nInstall with: pip install requests", file=sys.stderr)
    sys.exit(1)


REST_BASE = "https://api.github.com"
GRAPHQL_URL = "https://api.github.com/graphql"

DEFAULT_QUERIES = [
    '"open to work" in:bio',
    '"available for work" in:bio',
    '"looking for opportunities" in:bio',
    '"seeking new role" in:bio',
    '(freelance OR contractor OR contract) in:bio',
]

TECH_KEYWORDS = {
    "python", "java", "javascript", "typescript", "react", "angular", "vue", "node",
    "golang", "go", "rust", "c#", ".net", "dotnet", "aws", "azure", "gcp",
    "docker", "kubernetes", "ai", "ml", "machine learning", "llm", "backend",
    "frontend", "full stack", "fullstack", "devops", "data engineer", "data science",
    "postgres", "sql", "fastapi", "django", "flask", "spring", "terraform"
}

AVAILABILITY_PATTERNS = [
    r"\bopen to work\b",
    r"\bavailable for work\b",
    r"\bavailable\b",
    r"\blooking for opportunities\b",
    r"\bseeking (a )?new role\b",
    r"\bopen to opportunities\b",
    r"\bfreelance\b",
    r"\bcontract(or)?\b",
]


@dataclass
class Candidate:
    login: str
    name: str = ""
    bio: str = ""
    location: str = ""
    company: str = ""
    website_url: str = ""
    followers: int = 0
    public_repos: int = 0
    pinned_repo_names: str = ""
    top_languages: str = ""
    recent_repo_names: str = ""
    recent_repo_count: int = 0
    matching_keywords: str = ""
    availability_signal: bool = False
    score: int = 0
    profile_url: str = ""
    notes: str = ""


def get_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-candidate-miner"
    }


def rest_get(url: str, token: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resp = requests.get(url, headers=get_headers(token), params=params, timeout=30)
    handle_rate_limit(resp)
    resp.raise_for_status()
    return resp.json()


def graphql_post(query: str, variables: Dict[str, Any], token: str) -> Dict[str, Any]:
    resp = requests.post(
        GRAPHQL_URL,
        headers=get_headers(token),
        json={"query": query, "variables": variables},
        timeout=30
    )
    handle_rate_limit(resp)
    resp.raise_for_status()
    payload = resp.json()
    if "errors" in payload:
        raise RuntimeError(f"GraphQL error: {payload['errors']}")
    return payload["data"]


def handle_rate_limit(resp: requests.Response) -> None:
    if resp.status_code not in (403, 429):
        return

    remaining = resp.headers.get("X-RateLimit-Remaining")
    reset = resp.headers.get("X-RateLimit-Reset")
    if remaining == "0" and reset:
        wait_seconds = max(1, int(reset) - int(time.time()) + 1)
        print(f"Rate limit reached. Sleeping {wait_seconds} seconds...", file=sys.stderr)
        time.sleep(wait_seconds)
        return

    resp.raise_for_status()


def search_users(token: str, query: str, per_page: int = 30, max_pages: int = 2) -> List[str]:
    logins: List[str] = []
    seen = set()

    for page in range(1, max_pages + 1):
        data = rest_get(
            f"{REST_BASE}/search/users",
            token,
            params={"q": query, "per_page": per_page, "page": page},
        )
        for item in data.get("items", []):
            login = item.get("login")
            if login and login not in seen:
                seen.add(login)
                logins.append(login)

        if len(data.get("items", [])) < per_page:
            break

    return logins


GRAPHQL_USER_QUERY = """
query($login: String!) {
  user(login: $login) {
    login
    name
    bio
    location
    company
    websiteUrl
    url
    followers {
      totalCount
    }
    repositories(privacy: PUBLIC) {
      totalCount
    }
    pinnedItems(first: 6, types: REPOSITORY) {
      nodes {
        ... on Repository {
          name
          primaryLanguage {
            name
          }
          stargazerCount
        }
      }
    }
    recentRepositories: repositories(
      first: 12,
      privacy: PUBLIC,
      ownerAffiliations: OWNER,
      orderBy: {field: UPDATED_AT, direction: DESC}
    ) {
      nodes {
        name
        description
        stargazerCount
        updatedAt
        primaryLanguage {
          name
        }
      }
    }
  }
}
"""


def enrich_user(token: str, login: str) -> Candidate:
    data = graphql_post(GRAPHQL_USER_QUERY, {"login": login}, token)
    user = data.get("user")
    if not user:
        return Candidate(login=login, notes="User not found")

    pinned_nodes = user.get("pinnedItems", {}).get("nodes", []) or []
    recent_nodes = user.get("recentRepositories", {}).get("nodes", []) or []

    langs = []
    pinned_names = []
    recent_names = []

    for repo in pinned_nodes:
        if not repo:
            continue
        pinned_names.append(repo.get("name", ""))
        lang = ((repo.get("primaryLanguage") or {}).get("name") or "").strip()
        if lang:
            langs.append(lang)

    for repo in recent_nodes:
        if not repo:
            continue
        recent_names.append(repo.get("name", ""))
        lang = ((repo.get("primaryLanguage") or {}).get("name") or "").strip()
        if lang:
            langs.append(lang)

    langs = unique_preserve_order([x for x in langs if x])

    candidate = Candidate(
        login=user.get("login") or login,
        name=user.get("name") or "",
        bio=user.get("bio") or "",
        location=user.get("location") or "",
        company=user.get("company") or "",
        website_url=user.get("websiteUrl") or "",
        followers=int((user.get("followers") or {}).get("totalCount") or 0),
        public_repos=int((user.get("repositories") or {}).get("totalCount") or 0),
        pinned_repo_names=", ".join([x for x in pinned_names if x]),
        top_languages=", ".join(langs[:8]),
        recent_repo_names=", ".join([x for x in recent_names[:8] if x]),
        recent_repo_count=len([x for x in recent_names if x]),
        profile_url=user.get("url") or "",
    )
    return candidate


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        low = item.lower()
        if low not in seen:
            seen.add(low)
            result.append(item)
    return result


def extract_matching_keywords(text: str, repo_text: str) -> List[str]:
    haystack = f"{text} {repo_text}".lower()
    matches = []
    for kw in TECH_KEYWORDS:
        if kw.lower() in haystack:
            matches.append(kw)
    return sorted(set(matches), key=str.lower)


def has_availability_signal(text: str) -> bool:
    low = (text or "").lower()
    return any(re.search(pattern, low) for pattern in AVAILABILITY_PATTERNS)


def score_candidate(candidate: Candidate, target_keywords: List[str]) -> Candidate:
    text = " ".join([
        candidate.name,
        candidate.bio,
        candidate.company,
        candidate.location,
        candidate.top_languages,
    ])
    repo_text = candidate.recent_repo_names + " " + candidate.pinned_repo_names
    detected_keywords = extract_matching_keywords(text, repo_text)

    score = 0
    notes: List[str] = []

    availability = has_availability_signal(candidate.bio)
    if availability:
        score += 40
        notes.append("explicit availability signal")

    keyword_hits = 0
    for kw in target_keywords:
        if kw.lower() in (text + " " + repo_text).lower():
            keyword_hits += 1

    if keyword_hits:
        add = min(20, keyword_hits * 5)
        score += add
        notes.append(f"{keyword_hits} target keyword hit(s)")

    if detected_keywords:
        score += min(15, len(detected_keywords) * 2)
        notes.append("stack match")

    if candidate.recent_repo_count >= 6:
        score += 15
        notes.append("recent repo activity")
    elif candidate.recent_repo_count >= 3:
        score += 8
        notes.append("some recent activity")

    if candidate.website_url:
        score += 10
        notes.append("website/portfolio present")

    if candidate.followers >= 100:
        score += 5
        notes.append("higher follower count")
    elif candidate.followers >= 25:
        score += 2

    if candidate.public_repos >= 20:
        score += 5
        notes.append("substantial public repos")

    candidate.matching_keywords = ", ".join(detected_keywords)
    candidate.availability_signal = availability
    candidate.score = score
    candidate.notes = "; ".join(notes)
    return candidate


def parse_target_keywords(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [x.strip() for x in raw.split(",")]
    return [x for x in parts if x]


def build_query_from_parts(phrase: str = "", location: str = "", language: str = "", extra: str = "") -> str:
    parts: List[str] = []
    if phrase:
        phrase = phrase.strip()
        if "in:bio" in phrase:
            parts.append(phrase)
        else:
            parts.append(f'"{phrase}" in:bio')
    if location:
        parts.append(f"location:{location.strip()}")
    if language:
        parts.append(language.strip())
    if extra:
        parts.append(extra.strip())
    return " ".join([p for p in parts if p]).strip()


def export_csv(candidates: List[Candidate], output_path: str) -> None:
    fieldnames = list(asdict(candidates[0]).keys()) if candidates else list(Candidate(login="").__dict__.keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for candidate in candidates:
            writer.writerow(asdict(candidate))


def build_queries(args: argparse.Namespace) -> List[str]:
    queries: List[str] = []

    built_query = build_query_from_parts(
        phrase=args.phrase,
        location=args.location,
        language=args.language,
        extra=args.extra_query,
    )
    if built_query:
        queries.append(built_query)

    if args.query:
        queries.append(args.query)

    if args.use_defaults:
        queries.extend(DEFAULT_QUERIES)

    if not queries:
        queries = ['"open to work" in:bio']

    return queries


def dedupe_logins(items: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Single-file GitHub candidate miner")
    parser.add_argument("--query", help='Raw GitHub user search query. In PowerShell use double quotes around the whole query.')
    parser.add_argument("--phrase", default="", help='Phrase to search in bio, e.g. "open to work"')
    parser.add_argument("--location", default="", help='Location filter, e.g. canada or "toronto"')
    parser.add_argument("--language", default="", help='Optional stack/language term to include in the search, e.g. python')
    parser.add_argument("--extra-query", default="", help='Any extra GitHub search terms to append, e.g. followers:>10')
    parser.add_argument("--use-defaults", action="store_true", help="Also run a built-in set of public-availability queries")
    parser.add_argument("--keywords", default="", help="Comma-separated target role keywords, e.g. python,fastapi,aws,ai")
    parser.add_argument("--per-query-limit", type=int, default=30, help="Users to fetch per query page")
    parser.add_argument("--pages", type=int, default=1, help="Pages to fetch per query")
    parser.add_argument("--max-enrich", type=int, default=50, help="Maximum unique users to enrich")
    parser.add_argument("--min-score", type=int, default=0, help="Minimum score to keep")
    parser.add_argument("--output", default="github_candidates.csv", help="Output CSV path")
    args = parser.parse_args()

    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        print("Missing GITHUB_TOKEN environment variable.", file=sys.stderr)
        return 1

    queries = build_queries(args)
    target_keywords = parse_target_keywords(args.keywords)

    print("Searching GitHub public profiles...")
    if args.query:
        print("Tip: In PowerShell, raw --query values with spaces should usually be wrapped in double quotes.")
    elif args.phrase or args.location or args.language or args.extra_query:
        print("Built query mode enabled.")
    all_logins: List[str] = []
    for q in queries:
        print(f"  - {q}")
        try:
            logins = search_users(token, q, per_page=args.per_query_limit, max_pages=args.pages)
            all_logins.extend(logins)
        except Exception as exc:
            print(f"Search failed for query [{q}]: {exc}", file=sys.stderr)

    unique_logins = dedupe_logins(all_logins)[: args.max_enrich]
    print(f"Found {len(unique_logins)} unique users to enrich.")

    candidates: List[Candidate] = []
    for idx, login in enumerate(unique_logins, start=1):
        print(f"[{idx}/{len(unique_logins)}] Enriching {login}...")
        try:
            candidate = enrich_user(token, login)
            candidate = score_candidate(candidate, target_keywords)
            if candidate.score >= args.min_score:
                candidates.append(candidate)
        except Exception as exc:
            print(f"Failed to enrich {login}: {exc}", file=sys.stderr)

    candidates.sort(key=lambda c: c.score, reverse=True)
    export_csv(candidates, args.output)

    print(f"\nDone. Exported {len(candidates)} candidates to {args.output}")
    if candidates:
        print("\nTop candidates:")
        for c in candidates[:10]:
            display_name = c.name or c.login
            print(f"- {display_name} (@{c.login}) | score={c.score} | {c.profile_url}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
