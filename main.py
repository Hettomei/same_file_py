import hashlib
import os

with open(hashlib.__file__, "rb") as f:
    digest = hashlib.file_digest(f, "sha256")
    print(digest.hexdigest())
    print(hashlib.__file__)
    print(os.stat(hashlib.__file__).st_size, "bytes")
