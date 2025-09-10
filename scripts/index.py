import os
import json
import csv
import time
import logging
import datetime as dt
from io import StringIO
from typing import Dict, Iterable, List, Any, Optional
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")

DEFAULT_UA = "lambda-fetch/1.0 (+https://aws.amazon.com/lambda/)"

# ----------------- Helpers -----------------

def get_env(name: str, default: str = "") -> str:
    v = os.getenv(name, default)
    return v if v is not None else default

def ensure_trailing_slash(p: str) -> str:
    return p if not p or p.endswith("/") else p + "/"

def flatten_record(rec: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
    """Flatten nested dicts; lists become JSON strings."""
    items = []
    for k, v in rec.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_record(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        else:
            items.append((new_key, v))
    return dict(items)

def http_get_json(
    url: str,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    retries: int = 5,
    backoff: float = 0.5
) -> Any:
    attempt = 0
    merged_headers = {"User-Agent": DEFAULT_UA}
    if headers:
        merged_headers.update(headers)

    while True:
        try:
            req = Request(url, headers=merged_headers, method="GET")
            with urlopen(req, timeout=timeout) as r:
                payload = r.read()
                return json.loads(payload.decode("utf-8"))
        except HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            logger.error("HTTPError %s for %s. Body: %s", e.code, url, body)
            # Retry only transient errors
            if e.code in (429, 500, 502, 503, 504) and attempt < retries:
                attempt += 1
                sleep_s = backoff * (2 ** (attempt - 1))
                logger.warning("Retrying in %.1fs (%d/%d)...", sleep_s, attempt, retries)
                time.sleep(sleep_s)
                continue
            raise
        except (URLError, TimeoutError) as e:
            attempt += 1
            if attempt > retries:
                raise
            sleep_s = backoff * (2 ** (attempt - 1))
            logger.warning("Network error (%s). Retrying in %.1fs (%d/%d) url=%s", e, sleep_s, attempt, retries, url)
            time.sleep(sleep_s)

def write_csv_to_s3(rows: Iterable[Dict[str, Any]], bucket: str, key: str) -> None:
    fieldnames: List[str] = []
    buffered_rows = []
    for r in rows:
        buffered_rows.append(r)
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)

    csv_buf = StringIO()
    writer = csv.DictWriter(csv_buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for r in buffered_rows:
        writer.writerow({k: r.get(k, "") for k in fieldnames})

    s3.put_object(Bucket=bucket, Key=key, Body=csv_buf.getvalue().encode("utf-8"))
    logger.info("Uploaded %d rows to s3://%s/%s", len(buffered_rows), bucket, key)

def page_url(base_url: str, endpoint: str, page_param: str, page: int, page_size_param: str, page_size: int) -> str:
    params = {}
    if page_param:
        params[page_param] = page
    if page_size_param and page_size > 0:
        params[page_size_param] = page_size
    qs = urlencode(params)
    base = base_url.rstrip("/")
    path = endpoint.lstrip("/")
    return f"{base}/{path}" + (f"?{qs}" if qs else "")

def fetch_endpoint(
    base_url: str,
    endpoint: str,
    page_param: str,
    start_page: int,
    page_size_param: str,
    page_size: int,
    max_pages: int
) -> Iterable[Dict[str, Any]]:
    page = start_page
    fetched = 0
    while True:
        url = page_url(base_url, endpoint, page_param, page, page_size_param, page_size)
        logger.info("Fetching %s", url)
        data = http_get_json(url)

        # The API wraps lists in {"data":[...], "pagination": {...}}
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                batch = data["data"]
            else:
                batch = [data]
        elif isinstance(data, list):
            batch = data
        else:
            batch = [{"_payload": json.dumps(data, ensure_ascii=False)}]

        if not batch:
            logger.info("No data at page=%s; stopping endpoint=%s", page, endpoint)
            break

        for rec in batch:
            try:
                yield flatten_record(rec)
            except Exception as e:
                logger.warning("Skipping bad record: %s", e)

        fetched += 1
        if max_pages > 0 and fetched >= max_pages:
            logger.info("Reached max_pages=%d for endpoint=%s", max_pages, endpoint)
            break
        if not page_param:
            break
        page += 1

# ----------------- Lambda handler -----------------

def lambda_handler(event, context):
    bucket = get_env("BUCKET_NAME")
    if not bucket:
        raise RuntimeError("Missing BUCKET_NAME env var")

    base_url = get_env("BASE_URL", "https://fakeapi.net")
    # Use documented endpoints (no auth required)
    tables_csv = get_env("TABLE_ENDPOINTS", "/products,/users,/orders,/reviews")
    prefix = ensure_trailing_slash(get_env("S3_PREFIX", "fake_ecom/raw"))

    # Pagination per docs
    page_param      = get_env("PAGE_PARAM", "page")
    start_page      = int(get_env("START_PAGE", "1"))
    page_size_param = get_env("PAGE_SIZE_PARAM", "limit")
    page_size       = int(get_env("PAGE_SIZE", "100"))
    max_pages       = int(get_env("MAX_PAGES", "0"))  # 0 = no cap

    now = dt.datetime.utcnow()
    y, m, d = now.year, now.month, now.day
    run_ts = now.strftime("%Y%m%dT%H%M%SZ")

    total_rows = 0
    results = {}

    for endpoint in [e.strip() for e in tables_csv.split(",") if e.strip()]:
        table = endpoint.rstrip("/").split("/")[-1].lower()
        key_prefix = f"{prefix}{table}/{y:04d}/{m:02d}/{d:02d}/run={run_ts}/"
        key = f"{key_prefix}{table}_{run_ts}.csv"

        rows = list(fetch_endpoint(
            base_url=base_url,
            endpoint=endpoint,
            page_param=page_param,
            start_page=start_page,
            page_size_param=page_size_param,
            page_size=page_size,
            max_pages=max_pages
        ))

        write_csv_to_s3(rows, bucket=bucket, key=key)
        results[table] = {"rows": len(rows), "s3_key": key}
        total_rows += len(rows)

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "OK", "total_rows": total_rows, "results": results})
    }
