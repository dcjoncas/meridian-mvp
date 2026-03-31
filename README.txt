
Meridian multi-thread update

What changed:
- Added /api/outbox/{gmid}
- Added /api/threads/{gmid}
- member.html now shows three views:
  - Received
  - Sent
  - Accepted
- Mike can now see who accepted and choose which accepted thread to chat in
- The contacted person can do the same on their side

How to use:
1. Replace main.py and member.html with these versions
2. Keep your current ui.html and GMID.html from this package
3. Restart:
   uvicorn main:app --reload

Notes:
- Open My Inbox now works for Mike as a multi-thread view, not just a pure received-only inbox
- Accepted threads can be opened from either side
