Meridian MVP v13 (Stable)

Run:
  python -m venv .venv
  .venv\Scripts\activate
  pip install -r requirements.txt
  uvicorn main:app --reload

Open:
  http://127.0.0.1:8000

What’s new vs prior builds:
- Versioned database file: meridian_v13.db (no reset/wipe button needed)
- Automatic seeding of 50 synthetic profiles on startup if DB is underfilled
- Working flow: Match → Send Ping → open recipient member page → Accept/Decline → Chat on Accept
- Member simulation: "Open as Member" opens /member/<gmid> in a new tab/window
