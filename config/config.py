import os
from typing import Any, Dict
from urllib.parse import parse_qsl, urlparse

# Default sample/refresh interval in seconds.
# The Streamlit UI can override this with the sidebar slider,
# but this is the fallback / container default.
SAMPLE_INTERVAL = int(os.getenv("SENTRA_SAMPLE_INTERVAL", "2"))

# Default retention helper constants (used for purge buttons)
ONE_HOUR = 60 * 60
ONE_DAY = ONE_HOUR * 24
ONE_WEEK = ONE_DAY * 7

_DEFAULT_DATA_DIR = os.getenv("SENTRA_DATA_DIR", "/data")
_DB_URL = os.getenv("SENTRA_DB_URL")
_DB_HOST = os.getenv("SENTRA_DB_HOST", "mysql")
_DB_PORT = os.getenv("SENTRA_DB_PORT", "3306")
_DB_USER = os.getenv("SENTRA_DB_USER", "root")
_DB_PASSWORD = os.getenv("SENTRA_DB_PASSWORD", "root")
_DB_NAME = os.getenv("SENTRA_DB_NAME", "sentra")


def get_data_dir() -> str:
    """
    Returns the directory where sentra stores persistent artifacts (logs, caches, etc.).
    Ensures the directory exists.
    """
    candidate = os.path.abspath(_DEFAULT_DATA_DIR)
    os.makedirs(candidate, exist_ok=True)
    return candidate


def _parse_url(url: str) -> Dict[str, Any]:
    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query))
    return {
        "host": parsed.hostname or _DB_HOST,
        "port": parsed.port or int(_DB_PORT),
        "user": parsed.username or _DB_USER,
        "password": parsed.password or _DB_PASSWORD,
        "database": parsed.path.lstrip("/") or _DB_NAME,
        "charset": params.get("charset", "utf8mb4"),
        **params,
    }


def get_db_config() -> Dict[str, Any]:
    """
    Returns kwargs usable by mysql.connector.connect.
    """
    if _DB_URL:
        cfg = _parse_url(_DB_URL)
    else:
        cfg = {
            "host": _DB_HOST,
            "port": int(_DB_PORT),
            "user": _DB_USER,
            "password": _DB_PASSWORD,
            "database": _DB_NAME,
            "charset": "utf8mb4",
        }

    cfg.setdefault("autocommit", False)
    # Avoid raising on benign MySQL warnings such as
    # 1050 "Table 'X' already exists" when using
    # CREATE TABLE IF NOT EXISTS.
    cfg.setdefault("raise_on_warnings", False)
    cfg.setdefault("ssl_disabled", True)
    cfg.setdefault("use_pure", True)
    cfg.setdefault("connection_timeout", 30)
    return cfg


def get_db_summary() -> str:
    cfg = get_db_config()
    user = cfg.get("user", "root")
    host = cfg.get("host", "mysql")
    port = cfg.get("port", 3306)
    database = cfg.get("database", "sentra")
    return f"{user}@{host}:{port}/{database}"
