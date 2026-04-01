Meridian production member fix

What this package fixes now:
- Inbox now has tabs for Inbox / Accepted / Declined / Sent
- Member console can see accepted and declined items again
- Added My Network button and modal graph
- Reduced aggressive polling:
  - list refresh every 20 seconds only while tab visible
  - chat refresh every 8 seconds only while tab visible and accepted thread open
- Production mode on home:
  - removed Select Principal wording
  - keeps Mike as the authenticated session
  - added Invite New Member button
- Full Postgres backend included
- Preserves your current approved UI look and feel

Replace all files in your project root with these.
Then run:
$env:DATABASE_URL="postgresql://postgres:postgres@localhost:5432/meridian"
$env:APP_BASE_URL="http://127.0.0.1:8000"
pip install -r requirements.txt
uvicorn main:app --reload


Demo member login convention:
- Mike account: mike / red123
- Seeded demo members: member001 through member100
- Seeded demo member password: red123
