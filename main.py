import sqlite3
import hashlib
import os
import sys

import glob


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

# Sauvegarder tous les hash en memoire pour un acces plus rapide
# un hash de 64 bytes, x 800 k fichiers = 50 MO de mémoire dispo
# 64*800000/(1024*1024)
# 48.828 MO
def cache():
    pass


def main(arg_path):
    print(arg_path)
    with sqlite3.connect(
        "same_files.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
    ) as con:
        cur = con.cursor()
        initialize_db(cur)

        res = cur.execute("SELECT * FROM files")
        print(res.fetchall())

    for path in glob.iglob(os.path.join(arg_path, "**", "*"), recursive=True):
        print(path)
        if os.path.isfile(path):
            size_bytes = os.stat(path).st_size
            print(size_bytes, "bytes")
            with open(path, "rb") as f:
                hexdigest = hashlib.file_digest(f, "sha256").hexdigest()
                print(hexdigest)
                print("--")

                # cur.execute(
                #     """
                #     INSERT INTO files VALUES
                #         (null, ?, ?, ?, sha256)
                # """
                # )
                # cur.execute(
                #     "insert into files(id, path, name, size, sha256) values (?, ?, ?, ?, ?)",
                #     (None, now),
                # )
                # con.commit()
            # print(res.fetchone())


if __name__ == "__main__":
    main(sys.argv[1])
