import sqlite3
import hashlib
import os

with open(hashlib.__file__, "rb") as f:
    digest = hashlib.file_digest(f, "sha256")
    print(digest.hexdigest())
    print(hashlib.__file__)
    print(os.stat(hashlib.__file__).st_size, "bytes")

con = sqlite3.connect(
    "same_file.db", detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
)
cur = con.cursor()
cur.execute("CREATE TABLE files(path, name, size, sha256)")

res = cur.execute("SELECT * FROM sqlite_master")

res = cur.execute("SELECT * FROM movie")

print(res.fetchall())

cur.execute(
    """
    INSERT INTO movie VALUES
        ('Monty Python and the Holy Grail', 1975, 8.2),
        ('And Now for Something Completely Different', 1971, 7.5)
"""
)
con.commit()
print(res.fetchone())
