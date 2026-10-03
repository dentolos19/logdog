import base64
import hashlib
import hmac
import json
import os
import sys
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

os.environ["SECRET_KEY"] = "test-secret-with-at-least-thirty-two-characters"
for name in ("DATABASE_URL", "MEGABASE_URL"):
    os.environ[name] = "postgresql+psycopg://test:test@127.0.0.1/test"
for name in ("OPENROUTER_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_ENDPOINT_URL_S3", "AWS_SECRET_ACCESS_KEY"):
    os.environ[name] = "test"
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import jwt
from fastapi import HTTPException

from routes.auth import create_access_token, create_refresh_token, decode_token_payload, revoke_refresh_token


class TokenTests(unittest.TestCase):
    def test_existing_hs256_format(self):
        def encode(value):
            return base64.urlsafe_b64encode(json.dumps(value, separators=(",", ":")).encode()).rstrip(b"=")

        header = encode({"alg": "HS256", "typ": "JWT"})
        payload = encode({"sub": "user-1", "type": "access", "exp": int(datetime.now(UTC).timestamp()) + 60})
        body = header + b"." + payload
        signature = base64.urlsafe_b64encode(
            hmac.new(os.environ["SECRET_KEY"].encode(), body, hashlib.sha256).digest()
        ).rstrip(b"=")
        token = (body + b"." + signature).decode()
        self.assertEqual(decode_token_payload(token, "access")["sub"], "user-1")

    def test_invalid_signature_and_expiration(self):
        for key, expires in ((os.environ["SECRET_KEY"], -60), ("another-long-secret-key-for-test-only", 60)):
            token = jwt.encode(
                {"sub": "user-1", "type": "access", "exp": datetime.now(UTC) + timedelta(seconds=expires)},
                key,
                algorithm="HS256",
            )
            with self.assertRaises(HTTPException) as error:
                decode_token_payload(token, "access")
            self.assertEqual(error.exception.status_code, 401)

    def test_new_token_and_wrong_type(self):
        token = create_access_token("user-1")
        self.assertEqual(decode_token_payload(token, "access")["sub"], "user-1")
        with self.assertRaises(HTTPException):
            decode_token_payload(token, "refresh")

    def test_revoked_refresh_token(self):
        token = create_refresh_token("user-1")
        revoke_refresh_token(token)
        with self.assertRaises(HTTPException):
            decode_token_payload(token, "refresh")


if __name__ == "__main__":
    unittest.main()
