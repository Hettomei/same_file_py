import argparse
import glob
import hashlib
import os
from pprint import pprint
import sqlite3
import sys


def parse_args(sys_args):
    parser = argparse.ArgumentParser(
        prog="Same file", description="Find same file based on sha256"
    )
    parser.add_argument(
        "--feed-with",
        help="Root path for computing all sha256 digest",
        dest="root_path",
    )

    parser.add_argument(
        "--show-same-files",
        help="Explain which file are duplicated",
        dest="same_files",
        action="store_true",
    )

    parser.add_argument(
        "--refresh-same-files",
        help="Look at duplicated files in detabase. If file no more exist, remove from db",
        dest="refresh_files",
        action="store_true",
    )
    return parser.parse_args(sys_args)


def is_db_present(cur):
    res = cur.execute("SELECT * FROM sqlite_master")
    return len(res.fetchall()) > 0


def initialize_db(cur):
    if is_db_present(cur):
        print("db existante")
    else:
        print("creation db")
        cur.execute(
            """
CREATE TABLE files(
    id INTEGER PRIMARY KEY,
    path TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    sha256 TEXT NOT NULL
)
    """
        )

        cur.execute("CREATE INDEX idx_files_sha256 ON files (sha256)")


def main(args):
    with sqlite3.connect(
        "same_files.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    ) as con:
        cur = con.cursor()
        initialize_db(cur)
        if args.root_path:
            fill_database(con, cur, args)

        if args.refresh_files:
            refresh_same_files(cur)

        if args.same_files:
            show_same_files(cur)


def refresh_same_files(cur):
    for digest in extract_duplicated_digest(cur):
        ids_to_delete = []
        for _id, path, name, size, sha256 in get_same_digests(cur, digest):
            if not os.path.isfile(path):
                print(f"Inexistant: {size} Bytes, {path}")
                ids_to_delete.append((_id,))

        if ids_to_delete:
            cur.executemany("DELETE FROM files WHERE id = ?", ids_to_delete)
            print(f"Delete {cur.rowcount} row")

def extract_duplicated_digest(cur):
    # Extrait uniquement les doublons
    res = cur.execute(
        """
SELECT sha256, count(sha256) as count_sha256
FROM files
GROUP BY sha256
HAVING count_sha256 > 1
ORDER BY count_sha256 DESC
"""
    )
    # We extracted before the for ... so we free the cursor to do other SELECT
    return [row[0] for row in res]

def show_same_files(cur):
    for digest in extract_duplicated_digest(cur):
        start_explain(cur, digest)


def get_same_digests(cur, digest):
    return cur.execute(
        """
SELECT id, path, name, size, sha256
FROM files
WHERE sha256 = ?
ORDER BY path ASC
""",
        (digest,),
    )

def start_explain(cur, digest):
    for id, path, name, size, sha256 in get_same_digests(cur, digest):
        # print(row)
        print(f"{size} Bytes, {path}")

    print("--")


def fill_database(con, cur, args):
    for path in glob.iglob(os.path.join(args.root_path, "**", "*"), recursive=True):
        if os.path.isfile(path):
            size_bytes = os.stat(path).st_size
            with open(path, "rb") as f:
                _str = f"{size_bytes} Bytes, {path}"
                hexdigest = hashlib.file_digest(f, "sha256").hexdigest()
                try:
                    cur.execute(
                        "insert into files(id, path, name, size, sha256) values (?, ?, ?, ?, ?)",
                        (None, path, os.path.basename(path), size_bytes, hexdigest),
                    )
                    con.commit()
                except sqlite3.IntegrityError:
                    _str += ", already in dtb"
                print(_str)


if __name__ == "__main__":
    args = parse_args(sys.argv[1::1])
    main(args)
