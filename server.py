import os, json
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from openai import OpenAI

app = FastAPI()
client = OpenAI()

HTML_PATH = os.path.join(os.path.dirname(__file__), "vetting.html")

class VetIn(BaseModel):
    job_description: str
    resume_text: str

SYSTEM = """You are an expert technical recruiter and vetting interviewer.
Return STRICT JSON only (no markdown). Fill all fields.

Output schema:
{
  "overall_score": 0-100,
  "screening": {
    "technical_experience_notes": "string",
    "general_screening_notes": "string",
    "leadership_experience_years": number,
    "leadership_experience_team_size": number
  },
  "communication": {
    "proficiency_1_to_5": 1-5,
    "intelligibility_1_to_3": 1-3
  },
  "tech_skills": {
    "primary": ["..."],
    "secondary": ["..."]
  },
  "professional": {
    "level": "string",
    "title": "string"
  },
  "vetting_interview": {
    "syntax_familiarity_1_to_5": 1-5,
    "logical_methodology_1_to_5": 1-5,
    "pair_programming_integration_1_to_5": 1-5
  },
  "notes": {
    "technical_notes": "string",
    "extra_notes": "string"
  },
  "role_differentiator": "string",
  "vetting_result": "string"
}

Scoring guidance:
- Start from role fit, depth, recency, evidence, leadership, communication.
- Penalize missing must-haves, vague claims, weak evidence.
- Ensure ratings align with notes and overall_score.
"""

@app.get("/", response_class=HTMLResponse)
def home():
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        return f.read()

@app.post("/api/vet")
def vet(inp: VetIn):
    prompt = f"""JOB DESCRIPTION:
{inp.job_description}

RESUME:
{inp.resume_text}
"""
    rsp = client.responses.create(
        model="gpt-4.1-mini",
        input=[
            {"role":"system","content": SYSTEM},
            {"role":"user","content": prompt}
        ],
        temperature=0.2,
    )

    # Get text output, parse JSON safely
    text = rsp.output_text.strip()
    # Some models may wrap whitespace; must be JSON only
    result = json.loads(text)
    return {"result": result}
