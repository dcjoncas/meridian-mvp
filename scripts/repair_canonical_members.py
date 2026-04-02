#!/usr/bin/env python3
import argparse
import hashlib
import os
import re
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.getenv("DATABASE_URL")
SESSION_SECRET = os.getenv("SESSION_SECRET", "meridian-dev-session-secret-change-me")

ALIAS_ADJ = ["Blue","Red","Green","Swift","Bold","Wise","Fierce","Calm","Silent","Brave","Mystic","Dark","Light","Stormy","Fiery","Icy","Golden","Silver","Shadow","Thunder","Ancient","Eternal","Vivid","Quiet","Loud","Sharp","Dull","Bright","Dim","Hot","Cold","Wet","Dry","Fast","Slow","Heavy","Strong","Weak","Tall","Short","Long","Brief","Deep","Shallow","Wide","Narrow","Old","New","Young","Aged","Pure","Tainted","Clear","Cloudy","Sunny","Rainy","Windy","Still","Wild","Tame","Free","Bound","Happy","Sad","Angry","Peaceful","Chaotic","Orderly","Elegant","Clumsy","Graceful","Awkward","Smart","Clever","Rich","Poor","Full","Empty","Open","Closed","Locked","Unlocked","Safe","Dangerous","Friendly","Hostile","Warm","Cool","Soft","Hard","Smooth","Rough","Shiny","Matte","Vibrant","Faded","Iron","Velvet"]
ALIAS_NOUN = ["Dragon","Phoenix","Tiger","Eagle","Wolf","Fox","Bear","Lion","Hawk","Raven","Shark","Panther","Owl","Falcon","Viper","Cobra","Lynx","Stag","Bull","Horse","Snake","Spider","Scorpion","Whale","Dolphin","Fish","Bird","Cat","Dog","Mouse","Bat","Deer","Elk","Moose","Rabbit","Hare","Squirrel","Beaver","Otter","Seal","Walrus","Penguin","Ostrich","Peacock","Parrot","Crow","Dove","Swan","Goose","Duck","Pig","Cow","Sheep","Goat","Donkey","Mule","Camel","Llama","Elephant","Rhino","Hippo","Giraffe","Zebra","Antelope","Buffalo","Bison","Yak","Monkey","Ape","Gorilla","Chimp","Lemur","Sloth","Koala","Kangaroo","Platypus","Turtle","Tortoise","Lizard","Gecko","Iguana","Alligator","Crocodile","Frog","Toad","Salamander","Newt","Butterfly","Moth","Bee","Wasp","Condor","Manta","Narwhal","Puma","Jaguar","Raptor","Coyote","Badger"]


def hash_password(password: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", (password or "").encode("utf-8"), SESSION_SECRET.encode("utf-8"), 150000).hex()


def slugify_username(value: str) -> str:
    base = re.sub(r"[^a-z0-9]+", ".", (value or "").lower()).strip(".")
    return base or "member"


def alias_from_gmid(gmid: str) -> str:
    c = re.sub(r"[^a-fA-F0-9]", "", gmid or "").lower().ljust(16, "0")
    return ALIAS_ADJ[int(c[:8], 16) % len(ALIAS_ADJ)] + ALIAS_NOUN[int(c[8:16], 16) % len(ALIAS_NOUN)]


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


def canonical_alias(cur, gmid: str, member_id: Optional[int] = None) -> str:
    base = alias_from_gmid(gmid)
    alias_name = base
    suffix = 2
    while True:
        if member_id is None:
            cur.execute("SELECT id FROM members WHERE alias_name=%s", (alias_name,))
        else:
            cur.execute("SELECT id FROM members WHERE alias_name=%s AND id<>%s", (alias_name, member_id))
        row = cur.fetchone()
        if not row:
            return alias_name
        alias_name = f"{base}{suffix}"
        suffix += 1


@dataclass
class ProtectedLogin:
    username: str
    member_id: Optional[int]
    password_hash: Optional[str]
    must_change_password: Optional[bool]


def connect():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required")
    return psycopg2.connect(DATABASE_URL)


def snapshot_protected_logins(cur, protected: List[str]) -> Dict[str, ProtectedLogin]:
    out: Dict[str, ProtectedLogin] = {}
    for username in protected:
        cur.execute(
            "SELECT member_id, password_hash, must_change_password FROM member_auth WHERE lower(username)=lower(%s)",
            (username,),
        )
        row = cur.fetchone()
        if row:
            out[username.lower()] = ProtectedLogin(username, row[0], row[1], row[2])
        else:
            out[username.lower()] = ProtectedLogin(username, None, None, None)
    return out


def ensure_member_profile_rows(cur):
    cur.execute(
        """
        INSERT INTO member_profiles (member_id)
        SELECT m.id
        FROM members m
        LEFT JOIN member_profiles p ON p.member_id = m.id
        WHERE COALESCE(m.status, 'active') <> 'ghost'
          AND p.member_id IS NULL
        """
    )
    return cur.rowcount


def repair_aliases(cur) -> Tuple[int, int]:
    updated = 0
    duplicates_fixed = 0
    # first, blank out duplicate aliases except the oldest member record retaining the alias
    cur.execute(
        """
        WITH ranked AS (
            SELECT id, alias_name,
                   ROW_NUMBER() OVER (PARTITION BY lower(alias_name) ORDER BY id ASC) AS rn
            FROM members
            WHERE alias_name IS NOT NULL AND alias_name <> ''
        )
        UPDATE members m
        SET alias_name = NULL
        FROM ranked r
        WHERE m.id = r.id AND r.rn > 1
        RETURNING m.id
        """
    )
    duplicates_fixed = cur.rowcount

    cur.execute(
        "SELECT id, gmid FROM members WHERE COALESCE(status, 'active') <> 'ghost' AND (alias_name IS NULL OR alias_name='') ORDER BY id ASC"
    )
    rows = cur.fetchall()
    for member_id, gmid in rows:
        cur.execute("UPDATE members SET alias_name=%s WHERE id=%s", (canonical_alias(cur, gmid, member_id), member_id))
        updated += 1
    return updated, duplicates_fixed


def report_pool(cur) -> Dict[str, int]:
    metrics: Dict[str, int] = {}
    cur.execute("SELECT COUNT(*) FROM members WHERE COALESCE(status, 'active') <> 'ghost'")
    metrics["visible_members"] = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM members WHERE COALESCE(status, 'active') <> 'ghost' AND COALESCE(alias_name,'') <> ''")
    metrics["visible_with_alias"] = cur.fetchone()[0]
    cur.execute(
        """
        SELECT COUNT(*)
        FROM members m
        LEFT JOIN member_profiles p ON p.member_id = m.id
        WHERE COALESCE(m.status, 'active') <> 'ghost' AND p.member_id IS NOT NULL
        """
    )
    metrics["visible_with_profile"] = cur.fetchone()[0]
    cur.execute(
        """
        SELECT COUNT(*)
        FROM members m
        WHERE COALESCE(m.status, 'active') <> 'ghost' AND COALESCE(m.alias_name,'') <> ''
        """
    )
    metrics["canonical_pool"] = cur.fetchone()[0]
    return metrics


def find_alias_collisions(cur) -> List[Tuple[str, int]]:
    cur.execute(
        """
        SELECT lower(alias_name) AS alias_key, COUNT(*)
        FROM members
        WHERE COALESCE(status, 'active') <> 'ghost' AND alias_name IS NOT NULL AND alias_name <> ''
        GROUP BY lower(alias_name)
        HAVING COUNT(*) > 1
        ORDER BY COUNT(*) DESC, lower(alias_name)
        """
    )
    return cur.fetchall()


def find_members_missing_profiles(cur) -> List[Tuple[int, str]]:
    cur.execute(
        """
        SELECT m.id, m.gmid
        FROM members m
        LEFT JOIN member_profiles p ON p.member_id = m.id
        WHERE COALESCE(m.status, 'active') <> 'ghost'
          AND p.member_id IS NULL
        ORDER BY m.id ASC
        """
    )
    return cur.fetchall()


def ensure_protected_logins(cur, protected_state: Dict[str, ProtectedLogin], reset_known_passwords: bool):
    notes: List[str] = []

    def locate_member(username: str) -> Optional[int]:
        u = username.lower()
        if u == "mike":
            cur.execute("SELECT id FROM members WHERE lower(display_name)='mike s' OR lower(coalesce(email,''))='mike@meridian.local' ORDER BY id ASC LIMIT 1")
        elif u == "darrin.joncas":
            cur.execute("SELECT id FROM members WHERE lower(display_name)='darrin joncas' OR lower(coalesce(email,''))='darrin.joncas@gmail.com' ORDER BY id ASC LIMIT 1")
        elif u == "mack":
            cur.execute("""
                SELECT m.id
                FROM members m
                LEFT JOIN member_auth a ON a.member_id = m.id
                WHERE lower(coalesce(m.display_name,'')) LIKE 'mack%'
                   OR lower(coalesce(m.email,'')) LIKE 'mack%'
                   OR lower(coalesce(a.username,''))='mack'
                ORDER BY m.id ASC
                LIMIT 1
            """)
        else:
            cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s)", (username,))
        row = cur.fetchone()
        return row[0] if row else None

    for key, state in protected_state.items():
        target_member_id = state.member_id or locate_member(state.username)
        if not target_member_id:
            notes.append(f"Protected login '{state.username}' was not found and was left unchanged.")
            continue

        cur.execute("SELECT member_id, password_hash, must_change_password FROM member_auth WHERE lower(username)=lower(%s)", (state.username,))
        row = cur.fetchone()
        if row and row[0] == target_member_id:
            if reset_known_passwords and state.username.lower() in {"mike", "darrin.joncas"}:
                cur.execute(
                    "UPDATE member_auth SET password_hash=%s, must_change_password=FALSE WHERE member_id=%s",
                    (hash_password("red123"), target_member_id),
                )
                notes.append(f"Reset password for {state.username} to red123.")
            else:
                notes.append(f"Preserved existing login for {state.username}.")
            continue

        # free the username if it is attached elsewhere, but preserve that other login under a safe fallback username
        if row and row[0] != target_member_id:
            fallback_username = unique_username(cur, f"member{row[0]}", current_member_id=row[0])
            cur.execute("UPDATE member_auth SET username=%s WHERE member_id=%s", (fallback_username, row[0]))
            notes.append(f"Moved conflicting username '{state.username}' off member {row[0]} to '{fallback_username}'.")

        cur.execute("SELECT 1 FROM member_auth WHERE member_id=%s", (target_member_id,))
        has_auth = cur.fetchone() is not None
        password_hash_value = state.password_hash
        must_change = state.must_change_password if state.must_change_password is not None else False
        if reset_known_passwords and state.username.lower() in {"mike", "darrin.joncas"}:
            password_hash_value = hash_password("red123")
            must_change = False
        if not password_hash_value:
            password_hash_value = hash_password("red123") if state.username.lower() in {"mike", "darrin.joncas"} else hash_password("change-me")
            must_change = state.username.lower() not in {"mike", "darrin.joncas"}

        if has_auth:
            cur.execute(
                "UPDATE member_auth SET username=%s, password_hash=%s, must_change_password=%s WHERE member_id=%s",
                (state.username, password_hash_value, must_change, target_member_id),
            )
        else:
            cur.execute(
                "INSERT INTO member_auth (member_id, username, password_hash, must_change_password) VALUES (%s,%s,%s,%s)",
                (target_member_id, state.username, password_hash_value, must_change),
            )
        notes.append(f"Ensured protected login for {state.username} on member {target_member_id}.")
    return notes


def validate_protected_logins(cur, protected: List[str]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for username in protected:
        cur.execute("SELECT member_id FROM member_auth WHERE lower(username)=lower(%s)", (username,))
        row = cur.fetchone()
        result[username] = "ok" if row else "missing"
    return result


def main():
    parser = argparse.ArgumentParser(description="Repair Meridian canonical member pool and preserve protected logins.")
    parser.add_argument("--apply", action="store_true", help="Write the changes. Without this flag, the script runs in dry-run mode and rolls back.")
    parser.add_argument("--protect", default="mike,darrin.joncas,mack", help="Comma-separated usernames to preserve or reattach.")
    parser.add_argument("--reset-known-passwords", action="store_true", help="Reset Mike and darrin.joncas to red123 while preserving the username ownership.")
    args = parser.parse_args()

    protected = [x.strip() for x in args.protect.split(",") if x.strip()]
    conn = connect()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            protected_before = snapshot_protected_logins(cur, protected)
            before = report_pool(cur)
            alias_collisions_before = find_alias_collisions(cur)
            missing_profiles_before = find_members_missing_profiles(cur)

            # normalize membership state for canonical pool consistency
            cur.execute("UPDATE members SET status='active' WHERE status='pending_vetting'")
            status_promoted = cur.rowcount
            profiles_created = ensure_member_profile_rows(cur)
            aliases_set, alias_duplicates_cleared = repair_aliases(cur)
            login_notes = ensure_protected_logins(cur, protected_before, args.reset_known_passwords)

            after = report_pool(cur)
            alias_collisions_after = find_alias_collisions(cur)
            missing_profiles_after = find_members_missing_profiles(cur)
            protected_after = validate_protected_logins(cur, protected)

            print("=== Meridian Canonical Member Repair ===")
            print(f"Mode: {'APPLY' if args.apply else 'DRY RUN'}")
            print("\nBefore:")
            for k, v in before.items():
                print(f"  - {k}: {v}")
            print("\nActions:")
            print(f"  - pending_vetting promoted to active: {status_promoted}")
            print(f"  - profile rows created: {profiles_created}")
            print(f"  - duplicate aliases cleared: {alias_duplicates_cleared}")
            print(f"  - aliases assigned/reassigned: {aliases_set}")
            for note in login_notes:
                print(f"  - {note}")
            print("\nAfter:")
            for k, v in after.items():
                print(f"  - {k}: {v}")
            print("\nProtected login status:")
            for username, status in protected_after.items():
                print(f"  - {username}: {status}")

            if alias_collisions_before:
                print("\nAlias collisions before:")
                for alias_name, count in alias_collisions_before[:20]:
                    print(f"  - {alias_name}: {count}")
            if alias_collisions_after:
                print("\nAlias collisions after:")
                for alias_name, count in alias_collisions_after[:20]:
                    print(f"  - {alias_name}: {count}")
            if missing_profiles_before:
                print(f"\nMembers missing profiles before: {len(missing_profiles_before)}")
            if missing_profiles_after:
                print(f"Members missing profiles after: {len(missing_profiles_after)}")

            if args.apply:
                conn.commit()
                print("\nCommitted successfully.")
            else:
                conn.rollback()
                print("\nDry run complete. Rolled back all changes.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
