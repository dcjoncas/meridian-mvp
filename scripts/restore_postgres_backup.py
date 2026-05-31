#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

RESTORE_ORDER = [
    "members",
    "member_auth",
    "member_profiles",
    "member_documents",
    "member_ghost_snapshots",
    "member_invitations",
    "member_references",
    "pings",
    "chat_messages",
    "member_blocks",
    "password_reset_tokens",
    "admin_notes",
    "admin_audit_log",
]


def load_backup(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_json_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name=%s
          AND data_type IN ('json', 'jsonb')
        """,
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def get_target_columns(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name=%s
        """,
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


def table_exists(cur, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema='public'
          AND table_name=%s
          AND table_type='BASE TABLE'
        """,
        (table,),
    )
    return cur.fetchone() is not None


def init_schema():
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))
    import main as meridian_app

    meridian_app.init_schema()


def restore_backup(database_url: str, backup: dict, apply: bool):
    conn = psycopg2.connect(database_url, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            tables = backup.get("tables", {})
            available = [table for table in RESTORE_ORDER if table in tables]
            counts = {table: len(tables[table].get("rows", [])) for table in available}
            print("Backup rows:")
            for table in available:
                print(f"  {table}: {counts[table]}")

            if not apply:
                print("Dry run only. Re-run with --apply to restore into DATABASE_URL.")
                return

            for table in reversed(available):
                if table_exists(cur, table):
                    cur.execute(f"TRUNCATE TABLE {quote_ident(table)} RESTART IDENTITY CASCADE")

            for table in available:
                if not table_exists(cur, table):
                    print(f"Skipping missing table: {table}")
                    continue
                backup_columns = tables[table].get("columns", [])
                rows = tables[table].get("rows", [])
                if not backup_columns or not rows:
                    continue
                target_columns = get_target_columns(cur, table)
                columns = [column for column in backup_columns if column in target_columns]
                skipped_columns = [column for column in backup_columns if column not in target_columns]
                if skipped_columns:
                    print(f"Skipping unsupported columns in {table}: {', '.join(skipped_columns)}")
                if not columns:
                    print(f"Skipping {table}: no backup columns exist in target schema")
                    continue
                json_columns = get_json_columns(cur, table)
                col_sql = ", ".join(quote_ident(c) for c in columns)
                placeholders = ", ".join(["%s"] * len(columns))
                sql = f"INSERT INTO {quote_ident(table)} ({col_sql}) VALUES ({placeholders})"
                values = []
                for row in rows:
                    item = []
                    for column in columns:
                        value = row.get(column)
                        if column in json_columns and value is not None:
                            value = Json(value)
                        item.append(value)
                    values.append(item)
                cur.executemany(sql, values)
                print(f"Restored {len(rows)} rows into {table}")

            # Align all serial sequences after explicit id inserts.
            for table in available:
                if not table_exists(cur, table):
                    continue
                if "id" not in get_target_columns(cur, table):
                    continue
                cur.execute(
                    """
                    SELECT pg_get_serial_sequence(%s, 'id')
                    """,
                    (f"public.{table}",),
                )
                sequence = cur.fetchone()[0]
                if sequence:
                    cur.execute(
                        f"SELECT setval(%s::regclass, COALESCE((SELECT MAX(id) FROM {quote_ident(table)}), 1), true)",
                        (sequence,),
                    )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Restore a Meridian JSON backup into DATABASE_URL.")
    parser.add_argument("--backup", required=True, help="Path to a backup JSON created from Meridian Postgres.")
    parser.add_argument("--apply", action="store_true", help="Actually truncate and restore the target database.")
    parser.add_argument("--skip-init-schema", action="store_true", help="Do not run Meridian schema initialization first.")
    args = parser.parse_args()

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required for the target Postgres database.")

    backup = load_backup(Path(args.backup))
    if args.apply and not args.skip_init_schema:
        init_schema()
    restore_backup(database_url, backup, args.apply)


if __name__ == "__main__":
    main()
