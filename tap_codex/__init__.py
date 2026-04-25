import collections
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import argparse
import requests
import singer
from singer import bookmarks, metadata, metrics

session = requests.Session()
logger = singer.get_logger()

BASE_URL: str = "https://api.chatgpt.com/v1"
DAILY_USAGE_STREAM: str = "daily_usage"

REQUEST_TIMEOUT_SECONDS: int = 300
DEFAULT_PAGE_LIMIT: int = 1000
DEFAULT_SYNC_WINDOW_DAYS: int = 30

REQUIRED_CONFIG_KEYS: List[str] = ["start_date", "workspace_id"]

KEY_PROPERTIES: Dict[str, List[str]] = {
    DAILY_USAGE_STREAM: ["workspace_id", "start_time", "user_id"],
}

SUB_STREAMS: Dict[str, List[str]] = {}


class DependencyException(Exception):
    pass


class CodexException(Exception):
    pass


class BadCredentialsException(CodexException):
    pass


def translate_state(state: Dict[str, Any], catalog: Dict[str, Any]) -> Dict[str, Any]:
    nested_dict = lambda: collections.defaultdict(nested_dict)
    new_state: Dict[str, Any] = nested_dict()

    for stream in catalog["streams"]:
        stream_name = stream["tap_stream_id"]
        if bookmarks.get_bookmark(state, stream_name, "since"):
            new_state["bookmarks"][stream_name]["since"] = bookmarks.get_bookmark(
                state, stream_name, "since"
            )

    return new_state


def get_bookmark(
    state: Dict[str, Any], stream_name: str, bookmark_key: str, start_date: str
) -> str:
    stream_dict = bookmarks.get_bookmark(state, stream_name, bookmark_key)
    if stream_dict:
        return stream_dict
    return start_date


def get_stream_from_catalog(
    stream_id: str, catalog: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    for stream in catalog["streams"]:
        if stream["tap_stream_id"] == stream_id:
            return stream
    return None


def validate_dependencies(selected_stream_ids: List[str]) -> None:
    errs: List[str] = []
    msg_tmpl = (
        "Unable to extract '{0}' data, "
        "to receive '{0}' data, you also need to select '{1}'."
    )

    for main_stream, sub_streams in SUB_STREAMS.items():
        if main_stream not in selected_stream_ids:
            for sub_stream in sub_streams:
                if sub_stream in selected_stream_ids:
                    errs.append(msg_tmpl.format(sub_stream, main_stream))

    if errs:
        raise DependencyException(" ".join(errs))


def get_abs_path(path: str) -> str:
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def populate_metadata(schema_name: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    mdata = metadata.new()
    mdata = metadata.write(
        mdata, (), "table-key-properties", KEY_PROPERTIES[schema_name]
    )

    for field_name in schema["properties"].keys():
        if field_name in KEY_PROPERTIES[schema_name]:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "automatic"
            )
        else:
            mdata = metadata.write(
                mdata, ("properties", field_name), "inclusion", "available"
            )

    return mdata


def load_schemas() -> Dict[str, Dict[str, Any]]:
    schemas: Dict[str, Dict[str, Any]] = {}

    for filename in os.listdir(get_abs_path("schemas")):
        path = os.path.join(get_abs_path("schemas"), filename)
        file_raw = filename.replace(".json", "")
        with open(path, encoding="utf-8") as file:
            schemas[file_raw] = json.load(file)

    return schemas


def get_catalog() -> Dict[str, Any]:
    raw_schemas = load_schemas()
    streams: List[Dict[str, Any]] = []

    for schema_name, schema in raw_schemas.items():
        mdata = populate_metadata(schema_name, schema)
        catalog_entry = {
            "stream": schema_name,
            "tap_stream_id": schema_name,
            "schema": schema,
            "metadata": metadata.to_list(mdata),
            "key_properties": KEY_PROPERTIES[schema_name],
        }
        streams.append(catalog_entry)

    return {"streams": streams}


def get_request_timeout() -> float:
    args = singer.utils.parse_args([])
    config_request_timeout = args.config.get("request_timeout")
    if config_request_timeout and float(config_request_timeout):
        return float(config_request_timeout)
    return REQUEST_TIMEOUT_SECONDS


def authed_get(source: str, url: str, params: Dict[str, Any]) -> requests.Response:
    with metrics.http_request_timer(source) as timer:
        logger.info("Making GET request to %s with params %s", url, params)
        resp = session.request(
            method="get",
            url=url,
            params=params,
            timeout=get_request_timeout(),
        )
        logger.info("Request received status code %s", resp.status_code)
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        return resp


def _iso_to_unix_seconds(value: str) -> int:
    parsed: datetime = singer.utils.strptime_to_utc(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def _unix_seconds_to_iso(value: int) -> str:
    return singer.utils.strftime(
        datetime.fromtimestamp(int(value), tz=timezone.utc)
    )


def _iter_sync_windows(
    start_time_unix: int, end_time_unix: int, window_days: int
) -> List[tuple]:
    window_size_seconds = int(timedelta(days=window_days).total_seconds())
    windows: List[tuple] = []
    current_start_time_unix = start_time_unix

    while current_start_time_unix < end_time_unix:
        current_end_time_unix = min(
            current_start_time_unix + window_size_seconds, end_time_unix
        )
        windows.append((current_start_time_unix, current_end_time_unix))
        current_start_time_unix = current_end_time_unix

    return windows


def get_daily_usage(
    schema: Dict[str, Any],
    state: Dict[str, Any],
    mdata: List[Dict[str, Any]],
    start_date: str,
    workspace_id: str,
    group: Optional[str] = None,
    limit: int = DEFAULT_PAGE_LIMIT,
    window_days: int = DEFAULT_SYNC_WINDOW_DAYS,
) -> Dict[str, Any]:
    stream_name: str = DAILY_USAGE_STREAM
    bookmark_value: str = get_bookmark(state, stream_name, "since", start_date)
    next_window_start_time_unix: int = _iso_to_unix_seconds(bookmark_value)
    sync_end_time_unix: int = int(datetime.now(timezone.utc).timestamp())

    url: str = f"{BASE_URL}/analytics/codex/workspaces/{workspace_id}/usage"

    if next_window_start_time_unix >= sync_end_time_unix:
        logger.info(
            "No Codex data to sync for %s. next_window_start_time_unix=%s sync_end_time_unix=%s",
            stream_name,
            next_window_start_time_unix,
            sync_end_time_unix,
        )
        return state

    with metrics.record_counter(stream_name) as counter:
        for window_start_time_unix, window_end_time_unix in _iter_sync_windows(
            next_window_start_time_unix, sync_end_time_unix, window_days
        ):
            logger.info(
                "Fetching Codex usage from %s to %s",
                _unix_seconds_to_iso(window_start_time_unix),
                _unix_seconds_to_iso(window_end_time_unix),
            )

            page_cursor: Optional[str] = None

            while True:
                params: Dict[str, Any] = {
                    "start_time": window_start_time_unix,
                    "end_time": window_end_time_unix,
                    "limit": limit,
                }
                if group:
                    params["group"] = group
                if page_cursor:
                    params["page"] = page_cursor

                response = authed_get(stream_name, url, params)
                response.raise_for_status()
                payload: Dict[str, Any] = response.json()

                rows: List[Dict[str, Any]] = payload.get("data", [])
                extraction_time = singer.utils.now()

                for row in rows:
                    row_start_time: Optional[int] = row.get("start_time")
                    row_end_time: Optional[int] = row.get("end_time")
                    record: Dict[str, Any] = {
                        "workspace_id": workspace_id,
                        "start_time": row_start_time,
                        "end_time": row_end_time,
                        "start_date": _unix_seconds_to_iso(row_start_time)
                        if row_start_time is not None
                        else None,
                        "end_date": _unix_seconds_to_iso(row_end_time)
                        if row_end_time is not None
                        else None,
                        "user_id": row.get("user_id"),
                        "actor": row.get("actor"),
                        "totals": row.get("totals"),
                        "clients": row.get("clients", []),
                        "inserted_at": singer.utils.strftime(extraction_time),
                    }

                    try:
                        with singer.Transformer() as transformer:
                            rec = transformer.transform(
                                record,
                                schema,
                                metadata=metadata.to_map(mdata),
                            )
                    except Exception:
                        logger.exception("Failed to transform record [%s]", record)
                        raise

                    singer.write_record(
                        stream_name, rec, time_extracted=extraction_time
                    )
                    counter.increment()

                has_more: bool = bool(payload.get("has_more"))
                page_cursor = payload.get("next_page")

                if not has_more or not page_cursor:
                    break

            singer.write_bookmark(
                state,
                stream_name,
                "since",
                _unix_seconds_to_iso(window_end_time_unix),
            )
            singer.write_state(state)

    return state


def do_discover(config: Dict[str, Any]) -> None:
    catalog = get_catalog()
    print(json.dumps(catalog, indent=2))


def get_selected_streams(catalog: Dict[str, Any]) -> List[str]:
    selected_streams: List[str] = []
    for stream in catalog["streams"]:
        stream_metadata = stream["metadata"]
        if stream["schema"].get("selected", False):
            selected_streams.append(stream["tap_stream_id"])
        else:
            for entry in stream_metadata:
                if not entry["breadcrumb"] and entry["metadata"].get("selected", None):
                    selected_streams.append(stream["tap_stream_id"])

    return selected_streams


def do_sync(
    config: Dict[str, Any], state: Dict[str, Any], catalog: Dict[str, Any]
) -> None:
    api_key: Optional[str] = config.get("api_key")
    if not api_key:
        raise BadCredentialsException("No API key provided.")

    workspace_id: Optional[str] = config.get("workspace_id")
    if not workspace_id:
        raise CodexException("No workspace_id provided in config.")

    session.headers.update(
        {
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        }
    )

    start_date: str = config["start_date"]
    group: Optional[str] = config.get("group")
    limit: int = int(config.get("limit", DEFAULT_PAGE_LIMIT))
    window_days: int = int(config.get("window_days", DEFAULT_SYNC_WINDOW_DAYS))
    if window_days <= 0:
        raise CodexException("window_days must be greater than 0.")

    selected_stream_ids: List[str] = get_selected_streams(catalog)
    validate_dependencies(selected_stream_ids)

    state = translate_state(state, catalog)
    singer.write_state(state)

    for stream in catalog["streams"]:
        stream_id: str = stream["tap_stream_id"]
        stream_schema: Dict[str, Any] = stream["schema"]
        mdata: List[Dict[str, Any]] = stream["metadata"]

        if not SYNC_FUNCTIONS.get(stream_id):
            continue

        if stream_id in selected_stream_ids:
            singer.write_schema(stream_id, stream_schema, stream["key_properties"])
            sync_func = SYNC_FUNCTIONS[stream_id]
            state = sync_func(
                stream_schema,
                state,
                mdata,
                start_date,
                workspace_id,
                group,
                limit,
                window_days,
            )
            singer.write_state(state)


SYNC_FUNCTIONS = {
    DAILY_USAGE_STREAM: get_daily_usage,
}


@singer.utils.handle_top_exception(logger)
def main() -> None:
    cli_parser = argparse.ArgumentParser()
    cli_parser.add_argument("--config", type=str, default="config.json")
    cli_parser.parse_known_args()

    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    if not args.config.get("api_key"):
        env_api_key = os.getenv("CODEX_API_KEY")
        if env_api_key is not None:
            args.config["api_key"] = env_api_key
        else:
            raise BadCredentialsException("No API key provided.")

    if args.discover:
        do_discover(args.config)
    else:
        if args.properties:
            catalog = args.properties
        elif args.catalog:
            catalog = args.catalog.to_dict()
        else:
            catalog = get_catalog()

        do_sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
