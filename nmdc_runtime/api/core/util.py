import hashlib

from fastapi import HTTPException, status
from toolz import keyfilter


def omit(blacklist, d):
    return keyfilter(lambda k: k not in blacklist, d)


def pick(whitelist, d):
    return keyfilter(lambda k: k in whitelist, d)


def md5hash_from(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def raise404_if_none(doc, detail="Not found"):
    if doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail)
    return doc
