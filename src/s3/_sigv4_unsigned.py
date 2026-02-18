"""Подпись AWS SigV4 для S3 с x-amz-content-sha256: UNSIGNED-PAYLOAD.

Нужна для совместимости с бэкендами (Yandex Object Storage, MinIO за прокси и др.),
которые требуют именно UNSIGNED-PAYLOAD или валидный SHA256.
"""

import hashlib
import hmac
from datetime import datetime, timezone
from urllib.parse import quote


def _uri_encode(value: str, preserve_slash: bool = False) -> str:
    """URI-encode для S3: нерезервированные символы не трогаем, остальное в %XX (uppercase)."""
    safe = "/" if preserve_slash else ""
    s = quote(value, safe=safe, encoding="utf-8")
    result = []
    i = 0
    while i < len(s):
        if s[i] == "%" and i + 2 <= len(s):
            result.append("%" + s[i + 1 : i + 3].upper())
            i += 3
        else:
            result.append(s[i])
            i += 1
    return "".join(result)


def sign_s3_put_unsigned(
    *,
    key_id: str,
    key_secret: str,
    method: str,
    url_path: str,
    host: str,
    region: str = "us-east-1",
    amz_date: str | None = None,
) -> tuple[str, str, dict[str, str]]:
    """
    Строит подпись SigV4 для S3 PUT с UNSIGNED-PAYLOAD.

    Returns:
        (amz_date, authorization, headers_dict)
        headers_dict уже содержит host, x-amz-content-sha256, x-amz-date.
    """
    if amz_date is None:
        amz_date = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    date_stamp = amz_date[:8]
    payload_hash = "UNSIGNED-PAYLOAD"

    canonical_headers = (
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = (
        f"{method}\n"
        f"{url_path}\n"
        "\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = (
        f"{algorithm}\n"
        f"{amz_date}\n"
        f"{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode()).hexdigest()}"
    )

    def _hmac_sha256(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode(), hashlib.sha256).digest()

    k_date = _hmac_sha256(("AWS4" + key_secret).encode(), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, "s3")
    k_signing = _hmac_sha256(k_service, "aws4_request")
    signature = hmac.new(
        k_signing, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()

    authorization = (
        f"{algorithm} "
        f"Credential={key_id}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, "
        f"Signature={signature}"
    )

    headers = {
        "Host": host,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
        "Authorization": authorization,
    }
    return amz_date, authorization, headers


def build_put_url_and_path(
    endpoint_url: str, bucket: str, key: str
) -> tuple[str, str, str]:
    """
    Строит URL и path для path-style PUT.

    Returns:
        (full_url, host, canonical_uri_path)
    """
    from urllib.parse import urlparse

    parsed = urlparse(endpoint_url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or parsed.path
    host = netloc
    base = f"{scheme}://{netloc}".rstrip("/")

    path_encoded = "/" + _uri_encode(bucket, preserve_slash=False) + "/" + _uri_encode(key, preserve_slash=True)
    canonical_uri = path_encoded
    full_url = base + path_encoded
    return full_url, host, canonical_uri
