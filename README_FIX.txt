This ZIP contains the full main.py from the user's pasted source with only these surgical changes:
1) init_schema() seeds demo/system members only when the members table is empty.
2) member creation paths now use canonical_alias(cur, gmid) instead of alias_from_gmid(gmid) for persisted alias assignment.
Changed locations:
- Mike seed insert
- 100 system/demo seed loop
- /api/profile/create
- /api/invitations/{token}/complete
