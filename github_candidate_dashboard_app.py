#!/usr/bin/env python3
"""
github_candidate_dashboard_app.py

Run a Flask dashboard that lets you search public GitHub candidate profiles
from the browser and review ranked results instantly.

Requirements:
    pip install flask requests

PowerShell:
    $env:GITHUB_TOKEN="your_token_here"
    python github_candidate_dashboard_app.py

Then open:
    http://127.0.0.1:5000

Security note:
- Use only public, self-declared profile data
- Do not use for spam, bulk unsolicited outreach, or resale
"""

from __future__ import annotations

import csv
import io
import os
import re
import threading
import time
import webbrowser
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests
from flask import Flask, Response, jsonify, render_template_string, request

app = Flask(__name__)

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
    "postgres", "sql", "fastapi", "django", "flask", "spring", "terraform",
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

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>GitHub Candidate Dashboard</title>
  <style>
    :root{
      --bg:#0b1020;--panel:#121933;--panel-2:#182243;--text:#eef2ff;--muted:#aab4d6;
      --line:#2b396b;--accent:#6ea8fe;--accent-2:#7ef0c2;--warn:#ffcc66;--danger:#ff7b7b;
      --shadow:0 18px 45px rgba(0,0,0,.28);--radius:20px;
    }
    *{box-sizing:border-box}
    body{margin:0;font-family:Inter,Segoe UI,Arial,sans-serif;background:linear-gradient(180deg,#09101f 0%,#0e1530 100%);color:var(--text)}
    .wrap{max-width:1550px;margin:0 auto;padding:24px}
    .hero{
      display:flex;justify-content:space-between;gap:24px;align-items:flex-start;
      background:linear-gradient(135deg,rgba(110,168,254,.22),rgba(126,240,194,.12));
      border:1px solid rgba(255,255,255,.08);border-radius:28px;padding:26px 28px;
      box-shadow:var(--shadow);margin-bottom:22px;
    }
    .hero h1{margin:0 0 8px 0;font-size:34px;font-weight:800}
    .hero p{margin:0;color:var(--muted);max-width:900px;line-height:1.5}
    .actions{display:flex;gap:10px;flex-wrap:wrap}
    .btn{border:none;border-radius:14px;padding:12px 16px;font-weight:700;cursor:pointer;background:var(--accent);color:#09101f}
    .btn.secondary{background:#24345f;color:var(--text);border:1px solid rgba(255,255,255,.08)}
    .btn.ghost{background:transparent;color:var(--text);border:1px solid rgba(255,255,255,.12)}
    .btn:disabled{opacity:.55;cursor:not-allowed}
    .grid{display:grid;grid-template-columns:1.2fr 1fr 1fr 1fr;gap:18px;margin-bottom:18px}
    .card{background:rgba(18,25,51,.92);border:1px solid rgba(255,255,255,.08);border-radius:var(--radius);padding:18px;box-shadow:var(--shadow)}
    .metric .label{color:var(--muted);font-size:13px;margin-bottom:8px}
    .metric .value{font-size:34px;font-weight:800}
    .metric .sub{font-size:13px;color:var(--muted);margin-top:8px}
    .highlight{background:linear-gradient(135deg,rgba(110,168,254,.18),rgba(126,240,194,.08))}
    .layout{display:grid;grid-template-columns:380px 1fr;gap:18px;align-items:start}
    .section-title{font-size:14px;font-weight:800;letter-spacing:.3px;color:var(--muted);margin-bottom:12px;text-transform:uppercase}
    .filter-group{margin-bottom:16px}
    .filter-group label{display:block;font-size:13px;color:var(--muted);margin-bottom:8px}
    .input, select, textarea{
      width:100%;padding:12px 14px;border-radius:14px;border:1px solid rgba(255,255,255,.1);
      background:#0f1630;color:var(--text);outline:none
    }
    textarea{min-height:78px;resize:vertical}
    .two-col{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    .three-col{display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px}
    .range-row{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center}
    .chip{padding:8px 10px;border-radius:999px;background:#24345f;color:#dfe8ff;font-size:12px;border:1px solid rgba(255,255,255,.06)}
    .chips{display:flex;gap:8px;flex-wrap:wrap}
    .status{margin-top:10px;font-size:13px;color:var(--muted);line-height:1.5}
    .main-top{display:grid;grid-template-columns:1.1fr .9fr;gap:18px;margin-bottom:18px}
    .bar-list{display:grid;gap:12px}
    .bar-row .row-head{display:flex;justify-content:space-between;font-size:13px;color:#dbe4ff;margin-bottom:6px;gap:12px}
    .bar{height:12px;background:#0e1630;border-radius:999px;overflow:hidden;border:1px solid rgba(255,255,255,.05)}
    .bar > span{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,var(--accent),var(--accent-2));width:0%}
    .table-wrap{overflow:auto;border-radius:16px;border:1px solid rgba(255,255,255,.08)}
    table{width:100%;border-collapse:collapse;min-width:1250px;background:#101833}
    th,td{padding:14px 12px;text-align:left;border-bottom:1px solid rgba(255,255,255,.07);vertical-align:top}
    th{position:sticky;top:0;background:#16214a;font-size:12px;color:#c5d0f5;text-transform:uppercase;letter-spacing:.35px}
    td{font-size:14px;color:#eef2ff}
    tr:hover td{background:#131f44}
    .score{display:inline-flex;min-width:64px;justify-content:center;align-items:center;padding:8px 10px;border-radius:999px;font-weight:800}
    .score.high{background:rgba(126,240,194,.14);color:var(--accent-2)}
    .score.mid{background:rgba(255,204,102,.13);color:var(--warn)}
    .score.low{background:rgba(255,123,123,.12);color:var(--danger)}
    .tiny{font-size:12px;color:var(--muted)}
    .link{color:#9fc1ff;text-decoration:none}
    .link:hover{text-decoration:underline}
    .yes{color:var(--accent-2);font-weight:700}
    .no{color:var(--muted)}
    .empty{padding:44px 18px;text-align:center;color:var(--muted);border:1px dashed rgba(255,255,255,.12);border-radius:18px;background:#0f1630}
    .footer-note{margin-top:16px;color:var(--muted);font-size:12px;line-height:1.45}
    code{background:#0d1430;padding:2px 6px;border-radius:6px;border:1px solid rgba(255,255,255,.06)}
    @media (max-width: 1280px){.grid{grid-template-columns:1fr 1fr}.layout{grid-template-columns:1fr}.main-top{grid-template-columns:1fr}.hero{flex-direction:column}}
    @media (max-width: 760px){.grid{grid-template-columns:1fr}.two-col,.three-col{grid-template-columns:1fr}.wrap{padding:14px}.hero h1{font-size:28px}}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>GitHub Candidate Dashboard</h1>
        <p>
          Run GitHub candidate searches directly from this dashboard. No CSV upload required.
          The Python app serves this page, calls GitHub from Flask, scores candidates, and renders results instantly.
        </p>
      </div>
      <div class="actions">
        <button class="btn" id="runSearch">Run Search</button>
        <button class="btn secondary" id="exportServerCsv">Download Results CSV</button>
        <button class="btn ghost" id="useDefaults">Use Default Queries</button>
      </div>
    </div>

    <div class="grid">
      <div class="card metric highlight">
        <div class="label">Candidates in view</div>
        <div class="value" id="metricCount">0</div>
        <div class="sub" id="metricSummary">Run a search to begin.</div>
      </div>
      <div class="card metric">
        <div class="label">Average score</div>
        <div class="value" id="metricAvg">0</div>
        <div class="sub">Current filtered population</div>
      </div>
      <div class="card metric">
        <div class="label">Availability signals</div>
        <div class="value" id="metricAvail">0</div>
        <div class="sub">Explicit public “open to work” style wording</div>
      </div>
      <div class="card metric">
        <div class="label">Portfolio / website present</div>
        <div class="value" id="metricWeb">0</div>
        <div class="sub">Candidates with a public website URL</div>
      </div>
    </div>

    <div class="layout">
      <aside>
        <div class="card">
          <div class="section-title">Run search</div>

          <div class="filter-group">
            <label for="phrase">Phrase in bio</label>
            <input class="input" id="phrase" value="open to work" placeholder='open to work' />
          </div>

          <div class="two-col">
            <div class="filter-group">
              <label for="location">Location</label>
              <input class="input" id="location" value="canada" placeholder="canada" />
            </div>
            <div class="filter-group">
              <label for="language">Language / stack term</label>
              <input class="input" id="language" value="python" placeholder="python" />
            </div>
          </div>

          <div class="filter-group">
            <label for="keywords">Scoring keywords</label>
            <input class="input" id="keywords" value="python,fastapi,aws,ai" placeholder="python,fastapi,aws,ai" />
          </div>

          <div class="filter-group">
            <label for="extraQuery">Extra GitHub query terms</label>
            <input class="input" id="extraQuery" placeholder='followers:>10 repos:>5' />
          </div>

          <div class="three-col">
            <div class="filter-group">
              <label for="perQueryLimit">Per page</label>
              <input class="input" id="perQueryLimit" type="number" min="1" max="100" value="30" />
            </div>
            <div class="filter-group">
              <label for="pages">Pages</label>
              <input class="input" id="pages" type="number" min="1" max="10" value="1" />
            </div>
            <div class="filter-group">
              <label for="maxEnrich">Max enrich</label>
              <input class="input" id="maxEnrich" type="number" min="1" max="200" value="50" />
            </div>
          </div>

          <div class="filter-group">
            <label for="minScoreServer">Server-side min score</label>
            <input class="input" id="minScoreServer" type="number" min="0" max="100" value="0" />
          </div>

          <div class="status" id="runStatus">
            Make sure <code>GITHUB_TOKEN</code> is set in your terminal before you run the app.
          </div>
        </div>

        <div class="card" style="margin-top:18px">
          <div class="section-title">Client filters</div>

          <div class="filter-group">
            <label for="searchText">Search results</label>
            <input class="input" id="searchText" placeholder="name, login, bio, keywords, location..." />
          </div>

          <div class="filter-group">
            <label for="minScore">Minimum score</label>
            <div class="range-row">
              <input id="minScore" type="range" min="0" max="100" value="0" />
              <span id="minScoreValue" class="chip">0</span>
            </div>
          </div>

          <div class="two-col">
            <div class="filter-group">
              <label for="availabilityOnly">Availability</label>
              <select id="availabilityOnly">
                <option value="all">Show all</option>
                <option value="yes">Only yes</option>
                <option value="no">Only no</option>
              </select>
            </div>
            <div class="filter-group">
              <label for="websiteOnly">Website</label>
              <select id="websiteOnly">
                <option value="all">Show all</option>
                <option value="yes">Only yes</option>
                <option value="no">Only no</option>
              </select>
            </div>
          </div>

          <div class="filter-group">
            <label for="sortBy">Sort by</label>
            <select id="sortBy">
              <option value="score_desc">Score high to low</option>
              <option value="score_asc">Score low to high</option>
              <option value="followers_desc">Followers high to low</option>
              <option value="repos_desc">Public repos high to low</option>
              <option value="name_asc">Name A–Z</option>
              <option value="login_asc">Login A–Z</option>
            </select>
          </div>

          <div class="filter-group">
            <label>Keyword chips</label>
            <div class="chips" id="keywordChips"></div>
          </div>
        </div>
      </aside>

      <section>
        <div class="main-top">
          <div class="card">
            <div class="section-title">Top skill keywords in filtered set</div>
            <div class="bar-list" id="keywordBars"></div>
          </div>
          <div class="card">
            <div class="section-title">Top locations in filtered set</div>
            <div class="bar-list" id="locationBars"></div>
          </div>
        </div>

        <div class="card">
          <div class="section-title">Candidate table</div>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Candidate</th>
                  <th>Score</th>
                  <th>Availability</th>
                  <th>Followers</th>
                  <th>Repos</th>
                  <th>Location</th>
                  <th>Keywords</th>
                  <th>Website</th>
                  <th>Notes</th>
                </tr>
              </thead>
              <tbody id="candidateBody"></tbody>
            </table>
          </div>
          <div id="emptyState" class="empty">No candidates loaded yet.</div>
          <div class="footer-note">
            This dashboard calls your local Flask server, which calls GitHub using your <code>GITHUB_TOKEN</code>.
            Export downloads the current server results.
          </div>
        </div>
      </section>
    </div>
  </div>

<script>
const state = { raw: [], filtered: [], activeKeyword: null };

const els = {
  phrase: document.getElementById('phrase'),
  location: document.getElementById('location'),
  language: document.getElementById('language'),
  keywords: document.getElementById('keywords'),
  extraQuery: document.getElementById('extraQuery'),
  perQueryLimit: document.getElementById('perQueryLimit'),
  pages: document.getElementById('pages'),
  maxEnrich: document.getElementById('maxEnrich'),
  minScoreServer: document.getElementById('minScoreServer'),
  runSearch: document.getElementById('runSearch'),
  useDefaults: document.getElementById('useDefaults'),
  exportServerCsv: document.getElementById('exportServerCsv'),
  runStatus: document.getElementById('runStatus'),
  searchText: document.getElementById('searchText'),
  minScore: document.getElementById('minScore'),
  minScoreValue: document.getElementById('minScoreValue'),
  availabilityOnly: document.getElementById('availabilityOnly'),
  websiteOnly: document.getElementById('websiteOnly'),
  sortBy: document.getElementById('sortBy'),
  keywordChips: document.getElementById('keywordChips'),
  keywordBars: document.getElementById('keywordBars'),
  locationBars: document.getElementById('locationBars'),
  candidateBody: document.getElementById('candidateBody'),
  emptyState: document.getElementById('emptyState'),
  metricCount: document.getElementById('metricCount'),
  metricSummary: document.getElementById('metricSummary'),
  metricAvg: document.getElementById('metricAvg'),
  metricAvail: document.getElementById('metricAvail'),
  metricWeb: document.getElementById('metricWeb')
};

function scoreClass(score){
  if(score >= 60) return 'high';
  if(score >= 30) return 'mid';
  return 'low';
}
function splitKeywords(value){
  return String(value || '').split(',').map(x => x.trim()).filter(Boolean);
}
function includesText(candidate, q){
  const haystack = [
    candidate.login, candidate.name, candidate.bio, candidate.location,
    candidate.company, candidate.website_url, candidate.matching_keywords,
    candidate.top_languages, candidate.notes
  ].join(' ').toLowerCase();
  return haystack.includes(q);
}
function getKeywordCounts(rows){
  const map = new Map();
  rows.forEach(r => splitKeywords(r.matching_keywords).forEach(k => map.set(k, (map.get(k) || 0) + 1)));
  return [...map.entries()].sort((a,b) => b[1]-a[1]);
}
function getLocationCounts(rows){
  const map = new Map();
  rows.forEach(r => {
    const key = (r.location || 'Unknown').trim() || 'Unknown';
    map.set(key, (map.get(key) || 0) + 1);
  });
  return [...map.entries()].sort((a,b) => b[1]-a[1]);
}
function sortRows(rows, sortBy){
  const copy = [...rows];
  const compareText = (a,b,field) => String(a[field]||'').localeCompare(String(b[field]||''));
  if(sortBy === 'score_desc') copy.sort((a,b) => b.score-a.score);
  if(sortBy === 'score_asc') copy.sort((a,b) => a.score-b.score);
  if(sortBy === 'followers_desc') copy.sort((a,b) => b.followers-a.followers);
  if(sortBy === 'repos_desc') copy.sort((a,b) => b.public_repos-a.public_repos);
  if(sortBy === 'name_asc') copy.sort((a,b) => compareText(a,b,'name'));
  if(sortBy === 'login_asc') copy.sort((a,b) => compareText(a,b,'login'));
  return copy;
}
function renderBars(targetEl, items){
  targetEl.innerHTML = '';
  if(!items.length){
    targetEl.innerHTML = '<div class="tiny">No data available.</div>';
    return;
  }
  const max = items[0][1] || 1;
  items.slice(0, 8).forEach(([label, value]) => {
    const row = document.createElement('div');
    row.className = 'bar-row';
    row.innerHTML = `
      <div class="row-head"><span>${label}</span><span>${value}</span></div>
      <div class="bar"><span style="width:${(value/max)*100}%"></span></div>
    `;
    targetEl.appendChild(row);
  });
}
function renderKeywordChips(){
  const counts = getKeywordCounts(state.raw).slice(0, 18);
  els.keywordChips.innerHTML = '';
  if(!counts.length){
    els.keywordChips.innerHTML = '<span class="tiny">No keyword data loaded yet.</span>';
    return;
  }
  counts.forEach(([keyword, count]) => {
    const chip = document.createElement('button');
    chip.className = 'chip';
    chip.style.cursor = 'pointer';
    chip.style.background = state.activeKeyword === keyword ? 'rgba(126,240,194,.18)' : '';
    chip.style.color = state.activeKeyword === keyword ? 'var(--accent-2)' : '';
    chip.textContent = `${keyword} (${count})`;
    chip.onclick = () => {
      state.activeKeyword = state.activeKeyword === keyword ? null : keyword;
      applyFilters();
    };
    els.keywordChips.appendChild(chip);
  });
}
function renderMetrics(){
  const rows = state.filtered;
  const count = rows.length;
  const avg = count ? (rows.reduce((sum,r)=>sum+r.score,0)/count).toFixed(1) : '0';
  const avail = rows.filter(r => r.availability_signal).length;
  const web = rows.filter(r => !!r.website_url).length;
  els.metricCount.textContent = count;
  els.metricAvg.textContent = avg;
  els.metricAvail.textContent = avail;
  els.metricWeb.textContent = web;
  els.metricSummary.textContent = count
    ? `${avail} with explicit availability signals, ${web} with websites or portfolios.`
    : 'No candidates match the current filters.';
}
function renderTable(){
  const rows = state.filtered;
  els.candidateBody.innerHTML = '';
  els.emptyState.style.display = rows.length ? 'none' : 'block';
  rows.forEach(r => {
    const tr = document.createElement('tr');
    const displayName = r.name || r.login;
    tr.innerHTML = `
      <td>
        <div><a class="link" href="${r.profile_url || '#'}" target="_blank">${displayName}</a></div>
        <div class="tiny">@${r.login}</div>
        <div class="tiny">${r.company || ''}</div>
      </td>
      <td><span class="score ${scoreClass(r.score)}">${r.score}</span></td>
      <td>${r.availability_signal ? '<span class="yes">Yes</span>' : '<span class="no">No</span>'}</td>
      <td>${r.followers}</td>
      <td>${r.public_repos}</td>
      <td>${r.location || '<span class="tiny">—</span>'}</td>
      <td>${r.matching_keywords || '<span class="tiny">—</span>'}</td>
      <td>${r.website_url ? `<a class="link" href="${r.website_url}" target="_blank">Open</a>` : '<span class="tiny">—</span>'}</td>
      <td>${r.notes || '<span class="tiny">—</span>'}</td>
    `;
    els.candidateBody.appendChild(tr);
  });
}
function applyFilters(){
  const q = els.searchText.value.trim().toLowerCase();
  const minScore = Number(els.minScore.value || 0);
  const availabilityMode = els.availabilityOnly.value;
  const websiteMode = els.websiteOnly.value;
  const activeKeyword = state.activeKeyword;
  let rows = state.raw.filter(r => {
    if (r.score < minScore) return false;
    if (q && !includesText(r, q)) return false;
    if (availabilityMode === 'yes' && !r.availability_signal) return false;
    if (availabilityMode === 'no' && r.availability_signal) return false;
    if (websiteMode === 'yes' && !r.website_url) return false;
    if (websiteMode === 'no' && r.website_url) return false;
    if (activeKeyword && !splitKeywords(r.matching_keywords).map(x => x.toLowerCase()).includes(activeKeyword.toLowerCase())) return false;
    return true;
  });
  rows = sortRows(rows, els.sortBy.value);
  state.filtered = rows;
  renderMetrics();
  renderKeywordChips();
  renderBars(els.keywordBars, getKeywordCounts(state.filtered));
  renderBars(els.locationBars, getLocationCounts(state.filtered));
  renderTable();
}
function setData(rows){
  state.raw = rows || [];
  state.filtered = [...state.raw];
  applyFilters();
}
async function runSearch(useDefaults=false){
  els.runSearch.disabled = true;
  els.useDefaults.disabled = true;
  els.runStatus.textContent = 'Running GitHub search...';
  try{
    const payload = {
      phrase: els.phrase.value,
      location: els.location.value,
      language: els.language.value,
      keywords: els.keywords.value,
      extra_query: els.extraQuery.value,
      per_query_limit: Number(els.perQueryLimit.value || 30),
      pages: Number(els.pages.value || 1),
      max_enrich: Number(els.maxEnrich.value || 50),
      min_score: Number(els.minScoreServer.value || 0),
      use_defaults: useDefaults
    };
    const res = await fetch('/api/run-search', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(payload)
    });
    const data = await res.json();
    if(!res.ok){
      throw new Error(data.error || 'Search failed');
    }
    setData(data.candidates || []);
    els.runStatus.textContent = `Completed. Built query: ${data.query_summary || 'n/a'} | Returned ${data.candidates.length} candidates.`;
  }catch(err){
    els.runStatus.textContent = `Error: ${err.message}`;
  }finally{
    els.runSearch.disabled = false;
    els.useDefaults.disabled = false;
  }
}
els.runSearch.addEventListener('click', () => runSearch(false));
els.useDefaults.addEventListener('click', () => runSearch(true));
els.exportServerCsv.addEventListener('click', () => window.open('/api/export-last-search', '_blank'));
[els.searchText, els.minScore, els.availabilityOnly, els.websiteOnly, els.sortBy].forEach(el => {
  el.addEventListener('input', applyFilters);
  el.addEventListener('change', applyFilters);
});
els.minScore.addEventListener('input', () => { els.minScoreValue.textContent = els.minScore.value; });
setData([]);
</script>
</body>
</html>
"""


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


LAST_RESULTS: List[Dict[str, Any]] = []
LAST_QUERY_SUMMARY: str = ""


def get_token() -> str:
    return os.getenv("GITHUB_TOKEN", "").strip()


def get_headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "github-candidate-dashboard",
    }


def handle_rate_limit(resp: requests.Response) -> None:
    if resp.status_code not in (403, 429):
        return
    remaining = resp.headers.get("X-RateLimit-Remaining")
    reset = resp.headers.get("X-RateLimit-Reset")
    if remaining == "0" and reset:
        wait_seconds = max(1, int(reset) - int(time.time()) + 1)
        time.sleep(wait_seconds)
        return
    resp.raise_for_status()


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
        timeout=30,
    )
    handle_rate_limit(resp)
    resp.raise_for_status()
    payload = resp.json()
    if "errors" in payload:
        raise RuntimeError(f"GraphQL error: {payload['errors']}")
    return payload["data"]


def search_users(token: str, query: str, per_page: int = 30, max_pages: int = 1) -> List[str]:
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
          primaryLanguage { name }
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
        primaryLanguage { name }
      }
    }
  }
}
"""


def unique_preserve_order(items: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        low = item.lower()
        if low not in seen:
            seen.add(low)
            result.append(item)
    return result


def enrich_user(token: str, login: str) -> Candidate:
    data = graphql_post(GRAPHQL_USER_QUERY, {"login": login}, token)
    user = data.get("user")
    if not user:
        return Candidate(login=login, notes="User not found")

    pinned_nodes = user.get("pinnedItems", {}).get("nodes", []) or []
    recent_nodes = user.get("recentRepositories", {}).get("nodes", []) or []

    langs: List[str] = []
    pinned_names: List[str] = []
    recent_names: List[str] = []

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

    return Candidate(
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


def parse_target_keywords(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [x.strip() for x in raw.split(",")]
    return [x for x in parts if x]


def extract_matching_keywords(text: str, repo_text: str) -> List[str]:
    haystack = f"{text} {repo_text}".lower()
    matches: List[str] = []
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


def dedupe_logins(items: Iterable[str]) -> List[str]:
    seen = set()
    result = []
    for item in items:
        key = item.lower()
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def run_miner(
    phrase: str = "",
    location: str = "",
    language: str = "",
    keywords: str = "",
    extra_query: str = "",
    per_query_limit: int = 30,
    pages: int = 1,
    max_enrich: int = 50,
    min_score: int = 0,
    use_defaults: bool = False,
) -> Dict[str, Any]:
    token = get_token()
    if not token:
        raise RuntimeError("Missing GITHUB_TOKEN environment variable.")

    queries: List[str] = []
    built_query = build_query_from_parts(phrase=phrase, location=location, language=language, extra=extra_query)
    if built_query:
        queries.append(built_query)
    if use_defaults:
        queries.extend(DEFAULT_QUERIES)
    if not queries:
        queries = ['"open to work" in:bio']

    target_keywords = parse_target_keywords(keywords)

    all_logins: List[str] = []
    for q in queries:
        try:
            all_logins.extend(search_users(token, q, per_page=per_query_limit, max_pages=pages))
        except Exception:
            continue

    unique_logins = dedupe_logins(all_logins)[:max_enrich]

    candidates: List[Candidate] = []
    for login in unique_logins:
        try:
            candidate = enrich_user(token, login)
            candidate = score_candidate(candidate, target_keywords)
            if candidate.score >= min_score:
                candidates.append(candidate)
        except Exception:
            continue

    candidates.sort(key=lambda c: c.score, reverse=True)
    return {
        "queries": queries,
        "candidates": [asdict(c) for c in candidates],
    }


@app.route("/")
def index() -> str:
    return render_template_string(HTML)


@app.route("/api/run-search", methods=["POST"])
def api_run_search():
    global LAST_RESULTS, LAST_QUERY_SUMMARY

    payload = request.get_json(silent=True) or {}
    try:
        result = run_miner(
            phrase=str(payload.get("phrase", "")),
            location=str(payload.get("location", "")),
            language=str(payload.get("language", "")),
            keywords=str(payload.get("keywords", "")),
            extra_query=str(payload.get("extra_query", "")),
            per_query_limit=max(1, min(100, int(payload.get("per_query_limit", 30)))),
            pages=max(1, min(10, int(payload.get("pages", 1)))),
            max_enrich=max(1, min(200, int(payload.get("max_enrich", 50)))),
            min_score=max(0, min(100, int(payload.get("min_score", 0)))),
            use_defaults=bool(payload.get("use_defaults", False)),
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 400

    LAST_RESULTS = result["candidates"]
    LAST_QUERY_SUMMARY = " | ".join(result["queries"])
    return jsonify({
        "query_summary": LAST_QUERY_SUMMARY,
        "candidates": LAST_RESULTS,
    })


@app.route("/api/export-last-search")
def api_export_last_search():
    if not LAST_RESULTS:
        return jsonify({"error": "No results available yet. Run a search first."}), 400

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(LAST_RESULTS[0].keys()))
    writer.writeheader()
    for row in LAST_RESULTS:
        writer.writerow(row)

    csv_bytes = output.getvalue().encode("utf-8")
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=github_candidates.csv"},
    )


def open_browser():
    webbrowser.open_new("http://127.0.0.1:5000")


if __name__ == "__main__":
    token = get_token()
    if not token:
        print("ERROR: GITHUB_TOKEN is not set.")
        print("PowerShell example:")
        print('$env:GITHUB_TOKEN="your_new_token_here"')
        print("Then run:")
        print("python github_candidate_dashboard_app.py")
        raise SystemExit(1)

    threading.Timer(1.2, open_browser).start()
    app.run(host="127.0.0.1", port=5000, debug=False)
