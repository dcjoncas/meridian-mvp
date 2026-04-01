Meridian Postgres package

Included:
- Postgres-backed main.py
- invite_member.html
- complete_profile.html
- preserved current front-end pages
- Railway-ready requirements.txt
- railway.json
- .env.example

Key new features:
- DATABASE_URL Postgres connection
- invitation flow requiring a reference GMID
- complete-profile onboarding page
- room for richer member profile data
- member_documents table for uploaded documents
- member_invitations and member_references tables

Pages:
- /invite-member
- /invite/{token}
- /rankings
- /member/{gmid}

Railway setup:
1. Create a Postgres service in Railway
2. Copy DATABASE_URL into your app service variables
3. Set APP_BASE_URL to your public app URL
4. Deploy app service from GitHub

Local run:
uvicorn main:app --reload

Git:
git add .
git commit -m "Meridian: move to Postgres and add invitation onboarding"
git push origin main
