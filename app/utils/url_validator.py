from urllib.parse import urlparse
from fastapi import HTTPException

def validate_download_url(url: str) -> str:
    parsed = urlparse(url)

    # Enforce HTTPS only
    if parsed.scheme != "https":
        raise HTTPException(
            status_code=400,
            detail="Only HTTPS URLs are allowed"
        )

    hostname = parsed.hostname
    if not hostname:
        raise HTTPException(
            status_code=400,
            detail="Invalid URL: missing hostname"
        )

    # Reject punycode
    if hostname.startswith("xn--"):
        raise HTTPException(
            status_code=400,
            detail="Punycode domains are not allowed"
        )

    # Reject unicode / non-ASCII domains
    try:
        hostname.encode("ascii")
    except UnicodeEncodeError:
        raise HTTPException(
            status_code=400,
            detail="Unicode domains are not allowed"
        )

    return url