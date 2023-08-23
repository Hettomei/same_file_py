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

        if args.same_files:
            show_same_files(cur)


def show_same_files(cur):
    # res = cur.execute("SELECT * FROM files ORDER BY size DESC LIMIT 100")
    # pprint(res.fetchall())

    # print("-----------------")

    # res = cur.execute("SELECT * FROM files ORDER BY size ASC LIMIT 100")
    # pprint(res.fetchall())

    # afiche que les doublon
    res = cur.execute(
        """
SELECT sha256, count(sha256) as count_sha256
FROM files
GROUP BY sha256
HAVING count_sha256 > 1
ORDER BY count_sha256 DESC
LIMIT 3
"""
    )
    # We extracted before the for ... so we free the cursor to do other SELECT
    all_duplicated_digest = [row[0] for row in res]
    pprint(all_duplicated_digest)
    for digest in all_duplicated_digest:
        start_explain(cur, digest)


def start_explain(cur, digest):
    res = cur.execute(
        """
SELECT *
FROM files
WHERE sha256 = ?
ORDER BY path ASC
""",
        (digest,),
    )

    for row in res:
        print(row)
    print("----")


def fill_database(con, cur, args):
    for path in glob.iglob(os.path.join(args.root_path, "**", "*"), recursive=True):
        if os.path.isfile(path):
            print(path)
            size_bytes = os.stat(path).st_size
            print(size_bytes, "bytes")
            with open(path, "rb") as f:
                hexdigest = hashlib.file_digest(f, "sha256").hexdigest()
                print(hexdigest)
                try:
                    cur.execute(
                        "insert into files(id, path, name, size, sha256) values (?, ?, ?, ?, ?)",
                        (None, path, os.path.basename(path), size_bytes, hexdigest),
                    )
                    con.commit()
                except sqlite3.IntegrityError:
                    print("already in dtb")
                print("--")


if __name__ == "__main__":
    args = parse_args(sys.argv[1::1])
    main(args)
