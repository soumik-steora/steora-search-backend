"""
Search analytics helpers.

- Extracts the real client IP (handles proxies / load-balancers via django-ipware).
- Parses the User-Agent string into browser / OS / device info.
- Resolves the IP to geographic coordinates + ISP via ip-api.com (free, no key needed).
- Saves a SearchAnalytics row in a daemon thread so it never blocks the HTTP response.

ip-api.com limits: 45 req/min on the free HTTP endpoint.
For high-traffic or production use switch to ip-api.com Pro (HTTPS, no rate-limit)
or MaxMind GeoIP2 with a local database.
"""

import ipaddress
import logging
import threading

import requests
from ipware import get_client_ip
from user_agents import parse as parse_ua

logger = logging.getLogger(__name__)

_GEO_CACHE: dict = {}  # simple in-process cache keyed by IP
_GEO_CACHE_LOCK = threading.Lock()


def _normalize_ip(ip: str) -> str:
    """
    Convert IPv4-mapped IPv6 addresses (::ffff:192.168.x.x) to plain IPv4.
    Django / some OS networking stacks emit these when listening on a dual-stack socket.
    """
    try:
        addr = ipaddress.ip_address(ip)
        if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped:
            return str(addr.ipv4_mapped)
    except ValueError:
        pass
    return ip


def _is_private(ip: str) -> bool:
    """Return True for loopback, private, link-local, and reserved addresses."""
    try:
        addr = ipaddress.ip_address(ip)
        return (
            addr.is_private
            or addr.is_loopback
            or addr.is_link_local
            or addr.is_reserved
            or addr.is_unspecified
        )
    except ValueError:
        return True  # malformed → skip geo


def _geo_lookup(ip: str) -> dict:
    """
    Return geo data dict for a public IP.
    Uses ip-api.com free endpoint (HTTP only on free tier).
    Returns an empty dict on any error or for private IPs.
    """
    if not ip or _is_private(ip):
        return {}

    with _GEO_CACHE_LOCK:
        if ip in _GEO_CACHE:
            return _GEO_CACHE[ip]

    try:
        resp = requests.get(
            f"http://ip-api.com/json/{ip}",
            params={
                "fields": (
                    "status,message,country,countryCode,region,"
                    "regionName,city,lat,lon,isp,proxy,hosting,query"
                )
            },
            timeout=4,
        )
        data = resp.json()
    except Exception:
        logger.warning("ip-api.com lookup failed for %s", ip, exc_info=True)
        return {}

    if data.get("status") != "success":
        logger.debug("ip-api.com returned non-success for %s: %s", ip, data.get("message"))
        return {}

    result = {
        "city":         data.get("city", ""),
        "region":       data.get("regionName", ""),
        "country":      data.get("country", ""),
        "country_code": data.get("countryCode", ""),
        "latitude":     data.get("lat"),
        "longitude":    data.get("lon"),
        "isp":          data.get("isp", ""),
        "is_proxy":     bool(data.get("proxy", False)),
        "is_hosting":   bool(data.get("hosting", False)),
    }

    with _GEO_CACHE_LOCK:
        _GEO_CACHE[ip] = result

    return result


def _parse_device(ua_string: str) -> dict:
    """Return browser, OS, and device_type from a User-Agent string."""
    if not ua_string:
        return {"browser": "", "os": "", "device_type": "unknown"}

    ua = parse_ua(ua_string)

    if ua.is_bot:
        device_type = "bot"
    elif ua.is_mobile:
        device_type = "mobile"
    elif ua.is_tablet:
        device_type = "tablet"
    elif ua.is_pc:
        device_type = "desktop"
    else:
        device_type = "unknown"

    browser = ua.browser.family or ""
    if ua.browser.version_string:
        browser = f"{browser} {ua.browser.version_string}"

    os_name = ua.os.family or ""
    if ua.os.version_string:
        os_name = f"{os_name} {ua.os.version_string}"

    return {"browser": browser.strip(), "os": os_name.strip(), "device_type": device_type}


def _save_record(payload: dict) -> None:
    """Insert one SearchAnalytics row. Runs inside a background thread."""
    from .models import SearchAnalytics  # local import avoids circular deps at module load
    try:
        SearchAnalytics.objects.create(**payload)
    except Exception:
        logger.exception("Failed to persist search analytics record")


def _extract_ip_and_ua(request) -> tuple[str, str]:
    """
    Resolve the real client IP and User-Agent.

    Priority order for IP:
      1. X-Client-IP  — set by the Next.js Server Component when it proxies the
                        call, carrying the browser's real IP.
      2. X-Forwarded-For (first entry) — standard reverse-proxy header.
      3. django-ipware fallback — parses all known proxy headers automatically.

    Priority order for User-Agent:
      1. X-Client-UA  — forwarded by the Next.js Server Component.
      2. HTTP_USER_AGENT from the direct request.
    """
    # --- IP ---
    client_ip = (
        request.META.get("HTTP_X_CLIENT_IP", "").strip()
        or request.META.get("HTTP_X_FORWARDED_FOR", "").split(",")[0].strip()
    )

    if not client_ip:
        client_ip, _ = get_client_ip(request)

    if not client_ip:
        client_ip = "0.0.0.0"

    client_ip = _normalize_ip(client_ip)

    # --- User-Agent ---
    ua_string = (
        request.META.get("HTTP_X_CLIENT_UA", "").strip()
        or request.META.get("HTTP_USER_AGENT", "")
    )

    return client_ip, ua_string


def log_search(
    request,
    *,
    search_type: str,
    query: str = "",
    court_location: str = "",
    results_count: int = 0,
    latency_ms: int = 0,
) -> None:
    """
    Non-blocking analytics logging.

    Extracts IP + UA from the Django request synchronously, then fires a
    background daemon thread that does the geolocation lookup and DB write so
    the HTTP response is never delayed.
    """
    ip, ua_string = _extract_ip_and_ua(request)
    device_info = _parse_device(ua_string)

    payload = {
        "ip_address":    ip,
        "user_agent":    ua_string[:2000],  # guard against absurdly long strings
        "browser":       device_info["browser"][:120],
        "os":            device_info["os"][:120],
        "device_type":   device_info["device_type"],
        "search_type":   search_type,
        "query":         query[:500],
        "court_location": court_location[:255],
        "results_count": results_count,
        "latency_ms":    latency_ms,
        # geo fields filled in by background thread
        "city": "", "region": "", "country": "", "country_code": "",
        "latitude": None, "longitude": None,
        "isp": "", "is_proxy": False, "is_hosting": False,
    }

    def _worker():
        geo = _geo_lookup(ip)
        payload.update(geo)
        _save_record(payload)

    threading.Thread(target=_worker, daemon=True).start()
