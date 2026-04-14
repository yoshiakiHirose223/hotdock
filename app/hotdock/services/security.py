from __future__ import annotations

import hashlib
import hmac
import secrets
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import UTC, datetime, timedelta

from app.core.config import get_settings

settings = get_settings()


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def generate_token(size: int = 32) -> str:
    return urlsafe_b64encode(secrets.token_bytes(size)).decode("ascii").rstrip("=")


def _normalize_base64(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return urlsafe_b64decode(value + padding)


def hash_token(token: str) -> str:
    data = f"{token}:{settings.secret_key}".encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def verify_token_hash(token: str, token_hash: str) -> bool:
    return hmac.compare_digest(hash_token(token), token_hash)


def hash_password(password: str) -> str:
    salt = secrets.token_bytes(16)
    derived = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=64,
    )
    return f"scrypt${urlsafe_b64encode(salt).decode('ascii')}${urlsafe_b64encode(derived).decode('ascii')}"


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        scheme, salt_b64, digest_b64 = stored_hash.split("$", 2)
    except ValueError:
        return False
    if scheme != "scrypt":
        return False
    salt = _normalize_base64(salt_b64)
    expected = _normalize_base64(digest_b64)
    derived = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=2**14,
        r=8,
        p=1,
        dklen=len(expected),
    )
    return hmac.compare_digest(derived, expected)


def future_session_expiry() -> datetime:
    return utcnow() + timedelta(seconds=settings.auth_session_ttl_seconds)


def future_invitation_expiry() -> datetime:
    return utcnow() + timedelta(hours=settings.invitation_ttl_hours)


def future_claim_expiry() -> datetime:
    return utcnow() + timedelta(hours=settings.pending_claim_ttl_hours)
