# AI_CONTEXT.md

This document is a navigation and reasoning guide for AI coding agents working on the `Undying-Bit/monitor` repository.

Its purpose is to prevent an agent from wasting time scanning the entire codebase on every task. Start here, identify the relevant subsystem, then inspect only the files listed for that subsystem.

---

## 1. Project Summary

`monitor` is a Python asyncio monitoring service for broadcast signal messages. It ingests messages from Telegram and serial COM ports, preserves every source message as a raw event, parses each message into a normalized schema, classifies events against scheduled report windows, and writes results to SQLite for reporting and downstream dashboards.

The app has two primary ingestion modes:

1. Telegram ingestion, started with `python main.py`.
2. Serial/SAME/EAS ingestion, started with `python serial_main.py`.

Both ingestion paths produce the same core object, `RawEvent`, and feed it into the same processing pipeline. This is a core design invariant: source-specific code belongs at the edges; normalization and persistence should converge through `ProcessingService`.

---

## 2. Mental Model: Full Runtime Flow

The application is a pipe-and-filter pipeline:

```text
Telegram / Serial source
        |
        v
RawEvent
        |
        v
asyncio.Queue
        |
        v
Orchestrator
        |
        v
ProcessingService
        |
        +--> insert raw event
        +--> parse by source
        +--> resolve station identity
        +--> normalize into NormalizedEvent
        +--> insert normalized events
        +--> update raw parse status
        +--> resolve affected report slots
        |
        v
SQLite: mensajes.db
```

Important consequence: `Orchestrator` is not the business logic center. It is the queue consumer wrapper. The real hot path is `processing_service.py`.

---

## 3. Main Entry Points

### `main.py` — Telegram service entry point

Use this file when changing startup behavior for Telegram ingestion.

Responsibilities:

- Configure application logging.
- Parse CLI arguments:
  - `--telegram-catchup-overlap-messages`
  - `--telegram-catchup-last-days`
- Initialize `databases/mensajes.db` through `persistence.init_db()`.
- Create `StationManager`.
- Create shared `asyncio.Queue(maxsize=500)`.
- Start `Orchestrator` as the consumer task.
- Start `TelegramIngress` as the producer.
- Wait for Telegram disconnect or shutdown.

Do not place parser, database, or station-resolution business logic here. This file should remain wiring and lifecycle management.

### `serial_main.py` — Serial service entry point

Use this file when changing startup behavior for serial COM ingestion.

Responsibilities:

- Reuse `_setup_logging()` from `main.py`.
- Read configured serial ports from `SERIAL_PORTS`.
- Initialize DB and `StationManager`.
- Create the shared queue.
- Start `Orchestrator`.
- Start `SerialIngress`.
- Install process shutdown handlers for `SIGINT`, `SIGTERM`, and Windows `SIGBREAK` when available.
- Shut down the serial reader and queue consumer cleanly.

---

## 4. Configuration Surface

### `config.py`

Central config and environment loading. This is the first place to check for tunable runtime constants.

Important values:

```text
APP_VERSION
BASE_DIR
DATABASE_DIR
LOG_DIR
ENV_FILE
API_ID
API_HASH
GROUP_ID
SESSION_NAME
STATIONS_DB
PARSED_DB
LOCAL_TIMEZONE
TELEGRAM_CATCHUP_OVERLAP_MESSAGES
TELEGRAM_TIMEOUT_SECONDS
TELEGRAM_REQUEST_RETRIES
TELEGRAM_CONNECTION_RETRIES
TELEGRAM_RETRY_DELAY_SECONDS
SERIAL_PORTS
SERIAL_BAUDRATE
SERIAL_BYTESIZE
SERIAL_PARITY
SERIAL_STOPBITS
SERIAL_TIMEOUT_SECONDS
SERIAL_RECONNECT_DELAY_SECONDS
SERIAL_MAX_FRAME_BYTES
PHONE_LENGTH
DEFAULT_COUNTRY_CODE
CONFIDENCE_RESOLVE_THRESHOLD
STATE_PAIR_WINDOW_SECONDS
REPORT_HOURS
REPORT_MINUTE
TONO_WINDOW_SECONDS
APP_LOG
PARSING_ERRORS_LOG
SYSTEM_HEALTH_LOG
```

Current report schedule:

```text
02:45
05:45
08:45
11:45
14:45
17:45
20:45
23:45
```

The tone/report window is forward-only:

```text
window_start <= event_time <= window_start + TONO_WINDOW_SECONDS
```

Currently `TONO_WINDOW_SECONDS = 120`, so `05:45:00` through `05:47:00` is valid, but `05:44:59` and `05:47:01` are invalid.

### `.env.example`

Documents environment variables. Do not put real credentials in the repo.

Contains:

- Telegram API credentials.
- Telegram group/channel ID.
- Session name.
- Telegram reconnect/catch-up tuning.
- Local timezone.
- Serial settings.
- Station resolution threshold.

---

## 5. Core Data Models

### `models.py`

This file defines the vocabulary used across the app. Inspect this before modifying parser, normalization, persistence, or tests.

Objects:

#### `Source`

```python
TELEGRAM = "telegram"
SERIAL = "serial"
```

#### `MessageType`

Telegram classification values:

```python
OPEN
CLOSE
RWT
SINGLE
```

`RWT` is used for Type-B / `MENSAJE` test messages that are not open/close state messages.

#### `RawEvent`

Ingress-level representation for any source.

Important fields:

```python
source
source_event_id
raw_payload
received_at
transport_meta
```

All source data should be preserved as `RawEvent` before parsing. Do not skip raw preservation.

#### `TelegramParsed`

Intermediate Telegram parse result before station resolution.

Important fields:

```python
phone
timestamp
content
is_mensaje
mensaje_station_hint
mensaje_channel_raw
mensaje_text
```

#### `SerialParsed`

Intermediate SAME/EAS header parse result.

Important fields:

```python
originator
event_code
area_codes
duration_code
julian_day
hour
minute
transmitter_code
raw_header
repeat_count
```

#### `NormalizedEvent`

Unified event model ready for persistence.

Important fields:

```python
raw_event_id
source
station_id
station_name
station_code
event_type
event_class
report_date
report_slot
event_time_local
event_time_utc
tone
priority
is_valid_report
parser_version
confidence_score
transmitter_code
phone_number
channel
payload_json
```

`station_id` may be `None`. That is valid and intentional when station resolution is unknown or ambiguous.

---

## 6. Ingestion Layer

### Telegram: `telegram_client.py`

Use this file for Telegram connectivity, live message handling, and startup replay behavior.

Main class:

```python
TelegramIngress
```

Responsibilities:

- Create and start Telethon `TelegramClient`.
- Register a live `events.NewMessage(chats=GROUP_ID)` handler.
- Convert Telethon messages into `RawEvent`.
- Run catch-up before entering `run_until_disconnected()`.
- Monitor queue pressure.

Important behavior:

- Live handler is registered before catch-up begins. This avoids missing messages during startup replay.
- Normal restart catch-up uses `persistence.get_latest_telegram_message_id()` as a watermark.
- Configured overlap defaults to `TELEGRAM_CATCHUP_OVERLAP_MESSAGES`.
- CLI `--telegram-catchup-overlap-messages X` expands the overlap for that launch only using `max(configured_overlap, X)`.
- CLI `--telegram-catchup-last-days N` ignores the stored watermark for that launch and replays recent history by Telegram message date.
- Messages without text or without message ID are skipped.

Where to edit:

| Change needed | File |
|---|---|
| Telegram connection parameters | `config.py`, `telegram_client.py` |
| Which chat/group is monitored | `.env`, `GROUP_ID`, `telegram_client.py` |
| Catch-up logic | `telegram_client.py` |
| Startup replay CLI flags | `main.py`, `telegram_client.py` |
| Queue pressure logging | `telegram_client.py` |

### Serial: `serial_client.py`

Use this file for COM port reading, framing, threading, and serial-to-queue behavior.

Main class:

```python
SerialIngress
```

Responsibilities:

- Start one worker thread per configured serial port using `asyncio.to_thread()`.
- Open `serial.Serial(...)` with config values from `config.py`.
- Read bytes from the port.
- Buffer until the `NNNN` terminator is found.
- Decode each complete frame into a `RawEvent` with `source=Source.SERIAL`.
- Generate serial `source_event_id` as a trace ID:

```text
{port}:{received_at.isoformat()}:{seq}
```

- Detect oversized frames using `SERIAL_MAX_FRAME_BYTES`.
- Store oversized/overflow frames as raw events with `frame_overflow=True` and `terminator_found=False`.
- Bridge from serial worker threads into the asyncio queue using `asyncio.run_coroutine_threadsafe()`.
- Reconnect on `serial.SerialException` after `SERIAL_RECONNECT_DELAY_SECONDS`.

Important behavior:

- Serial `source_event_id` values are trace identifiers, not durable dedupe keys.
- The raw hash in persistence is still used for dedupe.
- Overflow does not carry bytes forward. The buffer is cleared to avoid contaminating the next frame.
- `_enqueue_blocking()` waits for queue capacity and exits cleanly if shutdown starts.

Where to edit:

| Change needed | File |
|---|---|
| COM port settings | `.env`, `config.py` |
| Frame terminator behavior | `serial_client.py` |
| Overflow behavior | `serial_client.py`, `processing_service.py` |
| Serial raw metadata | `serial_client.py` |
| SAME/EAS header parsing | `serial_parser.py` |

---

## 7. Queue Consumer

### `orchestrator.py`

This file is intentionally thin. It consumes `RawEvent` objects from the queue and delegates real work to `ProcessingService`.

Main class:

```python
Orchestrator
```

Responsibilities:

- Own an `asyncio.Queue[RawEvent]` reference.
- Create `ProcessingService`.
- Start the processing service.
- Run an infinite consumer loop.
- Call `ProcessingService.process_new_raw(raw)`.
- Log dedupe and processing result.
- Always call `queue.task_done()`.
- Close `ProcessingService` on shutdown.

Do not add parsing, normalization, or database writes here unless the pipeline architecture is intentionally changing.

---

## 8. Main Hot Path

### `processing_service.py`

This is the most important file for app behavior. Treat it as the transactional processing core.

Main class:

```python
ProcessingService
```

Main public methods:

```python
start()
close()
process_new_raw(raw)
reprocess_existing_raw(raw_event_id, raw, replace=False)
```

Core responsibilities:

1. Own a writer DB connection.
2. Retry transactional work on SQLite lock errors.
3. Insert raw events idempotently.
4. Parse raw payload by source.
5. Normalize parsed payload into one or more `NormalizedEvent` rows.
6. Persist normalized events.
7. Enforce first-valid-per-slot behavior for valid RWT tone events.
8. Update raw parse status.
9. Resolve affected report slots.
10. Preserve raw events on mid-pipeline failure.

Important internal methods:

```python
_process_new_raw_once()
_reprocess_existing_raw_once()
_process_existing_raw_in_tx()
_normalize_raw()
_resolve_slots()
```

Important behavior:

- `process_new_raw()` starts a transaction with `BEGIN IMMEDIATE`.
- If raw insert dedupes, it rolls back and returns `deduped=True`.
- If processing fails after raw insert, `record_processing_failure()` preserves evidence and marks raw status as `error`.
- Reprocessing can replace existing normalized events for a raw event when `replace=True`.
- Affected slots are tracked and re-resolved after changes.
- Telegram parsing uses `parse_telegram()` then `normalize_telegram()`.
- Serial parsing uses `parse_serial_payload()` then `normalize_serial()` for each parsed header.
- Serial overflow can yield `partial` status.
- Unknown source becomes `error` with `unknown_source:<source>`.

Critical invariant:

All live ingestion and reprocessing should go through `ProcessingService`. Do not create a second independent path for parsing and writing normalized records.

Where to edit:

| Change needed | File |
|---|---|
| End-to-end raw-to-normalized behavior | `processing_service.py` |
| Failure preservation | `processing_service.py`, `persistence.py` |
| Reprocessing semantics | `processing_service.py`, `reprocess_raw_events.py` |
| Serial partial/error status | `processing_service.py` |
| Source-specific normalization routing | `processing_service.py` |
| Slot re-resolution after edits | `processing_service.py`, `resolution_engine.py` |

---

## 9. Parsing Layer

### Telegram parser: `parser_engine.py`

Use this file when Telegram message text format changes.

Parsing is regex-based and two-tiered.

#### Tier 1

Regex:

```python
TIER1_RE
```

Expected shape:

```text
+PHONE DD/MM/YYYY H:MM:SS CONTENT
```

Extracts:

```text
phone
date
time
content
```

The parser normalizes Mexican country code by stripping leading `+` and leading `52`, then keeping `PHONE_LENGTH` digits.

#### Tier 2

Regex:

```python
TIER2_RE
```

Expected Type-B / `MENSAJE` shape:

```text
MENSAJE **/07/21 HH:MM:SS <text> STATION_NAME canal N
MENSAJE 01/07/21 HH:MM:SS <text> STATION_NAME CH-N
```

Extracts:

```text
mensaje_text
mensaje_station_hint
mensaje_channel_raw
```

Failures:

- Tier 1 failures are logged to `logs/parsing_errors.log` with a hash fingerprint and sanitized preview.
- Date parse failures also go to `parsing_errors.log`.
- Function returns `None` for unparseable messages.

Public helpers:

```python
parse_telegram(raw_text)
extract_channel_number(channel_raw)
```

Where to edit:

| Change needed | File |
|---|---|
| Telegram base format changed | `parser_engine.py` |
| `MENSAJE` format changed | `parser_engine.py` |
| Channel extraction changed | `parser_engine.py` |
| Parse-error logging format | `parser_engine.py` |
| Tests for Telegram parsing | `tests/test_parser.py` |

### Serial parser: `serial_parser.py`

Use this file when SAME/EAS header syntax changes.

Regex:

```python
HEADER_RE
```

Expected shape:

```text
ZCZC-ORG-EEE-AREAS+TTTT-JJJHHMM-STATION-
```

Examples:

```text
ZCZC-CIV-RWT-009000-015000+0300-1221200-XCMX/011-
ZCZC-CIV-EQW-000000+0001-1221200-XCMX/011-
```

Extracts:

```text
originator
event_code
area_codes
duration_code
julian_day
hour
minute
transmitter_code
raw_header
repeat_count
```

Important behavior:

- The parser finds all matching headers in the payload.
- Identical repeated headers are grouped and represented once with `repeat_count` set to the number of repeats.
- If no valid header is found, returns an empty list.

Where to edit:

| Change needed | File |
|---|---|
| SAME/EAS header format changed | `serial_parser.py` |
| Repeat counting changed | `serial_parser.py` |
| Serial parser tests | `tests/test_serial_parser.py` |

---

## 10. Normalization Layer

### `normalization.py`

This file converts source-specific parsed objects into the unified `NormalizedEvent` schema.

Main functions:

```python
normalize_telegram(parsed, raw_event_id, station_manager, resolver)
normalize_serial(parsed, raw_event_id, resolver, received_at)
validate_serial_timestamp_fields(parsed)
```

Important helper concepts:

- `_get_local_tz()` reads `LOCAL_TIMEZONE` and falls back safely.
- `_compute_slot()` delegates report-window detection to `schedule_engine.get_window_range()`.
- `_classify_telegram()` classifies Telegram messages into `OPEN`, `CLOSE`, `RWT`, or `SINGLE`.
- `_julian_to_datetime()` converts SAME/EAS Julian day + hour/minute into UTC time using the year inferred from `received_at`.

Telegram normalization behavior:

1. Resolve station from phone and optional `MENSAJE` station hint using `StationResolver.resolve_from_phone()`.
2. Classify as:
   - `RWT` / `TEST` for `MENSAJE` messages unless station `tx_sarmex == 2`.
   - `OPEN` / `STATE` if content matches station open text.
   - `CLOSE` / `STATE` if content matches station close text.
   - `SINGLE` / `INFO` otherwise.
3. Compute `report_date`, `report_slot`, and `tone` from the internal Telegram timestamp, not the receive time.
4. Calculate confidence from station resolution.
5. If station ambiguity confidence is below threshold, store `station_id=None`.
6. `is_valid_report=True` only when the event is inside a tone window and has a resolved station ID.

Serial normalization behavior:

1. Validate Julian day, hour, and minute.
2. Resolve station from transmitter code using `StationResolver.resolve_from_transmitter()`.
3. Infer event year from `received_at.year`.
4. Convert the SAME/EAS timestamp to local time.
5. Compute `report_date`, `report_slot`, and `tone` only for `RWT` events.
6. Classify event class:
   - `RWT` -> `TEST`
   - `EQW` -> `ALERT`
   - all others -> `INFO`
7. Set serial priority higher than Telegram priority.

Priorities currently implied by normalization:

```text
Telegram priority = 100
Serial priority   = 200
```

This matters when resolving slots because higher priority wins unless special OPEN/CLOSE logic applies.

Where to edit:

| Change needed | File |
|---|---|
| Event classification rules | `normalization.py` |
| Telegram open/close matching | `normalization.py`, `station_manager.py` |
| Serial timestamp inference | `normalization.py` |
| Priority rules | `normalization.py`, `resolution_engine.py` |
| `is_valid_report` logic | `normalization.py`, `schedule_engine.py` |
| Normalization tests | `tests/test_normalization.py` |

---

## 11. Schedule / Tono Logic

### `schedule_engine.py`

This file is the timing authority. Use it for report-window logic.

Main values/functions:

```python
TONO_WINDOWS
is_tono(msg_time)
get_window_range(msg_time)
get_active_windows()
```

`TONO_WINDOWS` is built from:

```python
REPORT_HOURS
REPORT_MINUTE
```

A message is considered `tone=True` only if its timestamp falls inside the forward-only window:

```text
window_start <= msg_time <= window_start + TONO_WINDOW_SECONDS
```

Current scheduled starts:

```text
02:45:00
05:45:00
08:45:00
11:45:00
14:45:00
17:45:00
20:45:00
23:45:00
```

Current valid end boundary with `TONO_WINDOW_SECONDS=120`:

```text
HH:47:00 inclusive
```

Current invalid examples:

```text
HH:44:59
HH:47:01
Any top-of-hour time like 04:00:00
Random off-schedule times like 10:30:00
```

Where to edit:

| Change needed | File |
|---|---|
| Report hours | `config.py` |
| Report minute | `config.py` |
| Window length | `config.py` |
| Window algorithm | `schedule_engine.py` |
| Schedule tests | `tests/test_schedule.py` |

Critical invariant:

Do not implement report-window logic separately in another file. Use `schedule_engine.py` or add helper functions there.

---

## 12. Station Reference Data and Resolution

### `station_manager.py`

Loads and caches `databases/estaciones.db`.

Main class:

```python
StationManager
```

Station dataclass:

```python
Station(
    station_id,
    nombre,
    telefono,
    open_text,
    close_text,
    tx_sarmex,
    red,
)
```

Expected SQLite table:

```text
Estaciones
```

Required columns:

```text
station_id
nombre
telefono
open
close
tx sarmex
red
```

Important behavior:

- `station_id` is required.
- `station_id` must not be `NULL`.
- Duplicate `station_id` values are rejected at startup.
- Phone numbers are normalized by removing `+`, removing leading `52`, then keeping `PHONE_LENGTH` digits.
- Stations are cached by phone and by name.
- Multiple stations may share a phone number.

Public lookup methods:

```python
lookup_by_phone(phone)
lookup_stations_by_phone(phone)
get_station(station_name)
get_station_case_insensitive(station_name)
get_open_close(station_name)
get_red(station_name)
get_tx_sarmex(station_name)
resolve_ambiguity(phone, name_hint)
get_all_phones()
get_all_stations()
```

### `station_resolver.py`

Centralized source-specific station resolution.

Main class:

```python
StationResolver
```

Result object:

```python
StationResolution(
    station_id,
    station_name,
    station_code,
    channel,
    resolved_by,
    confidence_penalty,
    ambiguous,
)
```

Important constants:

```python
TRANSMITTER_MAP
LEGACY_STATION_ALIASES
```

Current transmitter mappings include:

```text
XCMX/011 -> Teuhtli, channel 3
XCMX/004 -> Cuajimalpa, channel 5
XCMX/005 -> Zacatenco, channel 7
XMEX/037 -> La Palma, channel 6
XCMX/003 -> CENAPRED, channel 1
XMEX/048 -> Jocotitlan, channel 2
```

Legacy alias:

```text
TEUTLI -> TEUHTLI
```

Telegram phone resolution behavior:

- No station for phone -> `station_id=None`, name `Estacion {phone}`, `resolved_by="unknown_phone"`.
- One station for phone -> resolved directly, `resolved_by="phone_unique"`.
- Multiple stations for phone + usable station hint -> resolved by hint, `resolved_by="phone_hint"`.
- Multiple stations for phone + no usable hint -> unresolved ambiguity, `station_id=None`, `resolved_by="phone_ambiguous_unresolved"`, confidence penalty applied.

Serial transmitter resolution behavior:

- Known transmitter code -> map to station name/channel, then match station in `StationManager`.
- Unknown transmitter code -> `station_id=None`, name `Transmitter {code}`, `resolved_by="unknown_transmitter"`.

Where to edit:

| Change needed | File |
|---|---|
| Station DB schema expectations | `station_manager.py`, tests |
| Phone normalization | `station_manager.py`, `parser_engine.py` |
| Ambiguous station handling | `station_resolver.py`, `normalization.py` |
| Transmitter-code mapping | `station_resolver.py` |
| Station aliases | `station_resolver.py` |
| Station tests | `tests/test_station_manager.py`, `tests/test_normalization.py` |

---

## 13. Persistence Layer

### `persistence.py`

This is the SQLite ownership layer. Use this file for schema, migrations, inserts, projections, state-pair metadata, and query helpers.

Main responsibilities:

- Create DB connections.
- Enable WAL mode.
- Set `synchronous=NORMAL`.
- Set `busy_timeout=5000`.
- Enable foreign keys.
- Create/migrate tables.
- Insert raw events idempotently.
- Insert normalized events.
- Maintain projection tables `pruebas` and `alertas`.
- Maintain state-pair metadata for OPEN/CLOSE pairs.
- Provide query helpers for downstream reporting.
- Retry standalone DB operations on lock errors.

Primary tables:

#### `raw_events`

Raw audit/event table.

Important columns:

```text
id
source
source_event_id
received_at
raw_payload
raw_hash
transport_meta_json
parse_status
parse_error
```

Important constraints/indexes:

- `raw_hash` is unique.
- Telegram source events also have a partial unique index on `(source, source_event_id)` when `source='telegram'` and `source_event_id IS NOT NULL`.

#### `normalized_events`

Canonical parsed/normalized event table.

Important columns:

```text
id
raw_event_id
source
station_id
station_name
station_code
report_date
report_slot
event_type
event_class
channel
tone
priority
is_valid_report
event_time_utc
event_time_local
confidence_score
parser_version
transmitter_code
phone_number
payload_json
```

#### `pruebas`

Projection table for off-schedule events where `report_slot IS NULL` and `tone=0`, except EQW alert logic has special handling.

#### `alertas`

Projection table for `EQW` alert events where `report_slot IS NULL`.

#### `resolved_report_slots`

Deterministic per-station per-day per-slot winner table.

Important columns:

```text
station_id
station_name
report_date
report_slot
effective_event_id
effective_source
effective_event_type
effective_confidence
first_seen_at
last_updated_at
```

Important migration behavior:

- Older `raw_events` schemas with unique `(source, source_event_id)` are migrated to the current source-aware/raw-hash dedupe rules.
- Interrupted migrations involving `raw_events_new` are repaired or rejected depending on detected state.
- Legacy `parse_status='parsed'` rows are normalized to `complete`.
- Legacy triggers for `pruebas` and `alertas` are dropped because projection sync is now handled in code.

Important helper concepts:

- `retry_on_locked()` retries only when no shared DB connection is supplied. Shared transactions should not sleep/retry inside the transaction.
- `record_processing_failure()` runs in its own transaction to preserve audit evidence after a hot-path rollback.
- State-pair metadata tracks OPEN/CLOSE relationship in `payload_json`.

Where to edit:

| Change needed | File |
|---|---|
| DB schema | `persistence.py` |
| Migration behavior | `persistence.py` |
| Raw-event dedupe | `persistence.py` |
| Projection table behavior | `persistence.py` |
| Query helper behavior | `persistence.py` |
| SQLite lock/retry behavior | `persistence.py`, `processing_service.py` |
| Persistence tests | `tests/test_processing_service.py`, any DB-focused tests |

Critical invariants:

1. Do not remove raw-event preservation.
2. Do not bypass dedupe without replacing it with equivalent idempotency.
3. Do not perform retry sleeps inside an already-open shared transaction.
4. Keep `station_id=NULL` rows for audit and future reprocessing.
5. Projection table updates must stay consistent with `normalized_events`.

---

## 14. Slot Resolution

### `resolution_engine.py`

This file deterministically picks the effective event for a station/date/slot.

Main function:

```python
resolve_slot(station_id, report_date, report_slot, db_path=None, db=None)
```

Important behavior:

- Ignores invalid or incomplete slot identifiers.
- Looks at `normalized_events` where:

```text
station_id = ?
report_date = ?
report_slot = ?
is_valid_report = 1
```

- Orders candidates by:

```text
priority DESC
confidence_score DESC
event_time_local ASC
id ASC
```

- If there are both valid `OPEN` and valid `CLOSE` events in the slot, it prefers the earliest valid `OPEN`.
- If no valid winners remain, it deletes the corresponding row from `resolved_report_slots`.
- Otherwise it upserts the selected winner into `resolved_report_slots`.
- Preserves `first_seen_at` if a resolved slot already exists.

Where to edit:

| Change needed | File |
|---|---|
| Winner selection rules | `resolution_engine.py` |
| OPEN/CLOSE preference behavior | `resolution_engine.py` |
| Slot upsert/delete behavior | `resolution_engine.py` |
| Resolution tests | `tests/test_resolution_engine.py` |

Critical invariant:

Do not manually update `resolved_report_slots` from parser or normalization code. Let `ProcessingService` identify affected slots and call `resolve_slot()`.

---

## 15. Reprocessing

### `reprocess_raw_events.py`

This script re-runs parsing/normalization for stored `raw_events`.

Use it when:

- Parser regex changes.
- Station resolution changes.
- Normalization rules change.
- You need to rebuild normalized rows for historical raw data.

CLI options:

```text
--source all|telegram|serial
--since YYYY-MM-DD or ISO timestamp
--until YYYY-MM-DD or ISO timestamp
--status pending|complete|partial|error
--limit N
--offset N
--replace
--dry-run
```

Important behavior:

- Calls `persistence.init_db()` before work.
- Loads `StationManager`.
- Creates `ProcessingService`.
- Fetches matching raw rows from `raw_events`.
- Converts DB rows back to `RawEvent`.
- Calls `ProcessingService.reprocess_existing_raw()` for each row.
- If `--replace` is supplied, existing normalized rows for each raw event are deleted before rebuilding.
- If `--dry-run` is supplied, rows are selected but not written.
- Corrupted `transport_meta_json` is tolerated and replaced with `{}`.

Common commands:

```bash
python reprocess_raw_events.py --source telegram --since 2026-03-01 --replace
python reprocess_raw_events.py --source serial --status error --limit 500
python reprocess_raw_events.py --source all --dry-run
```

Where to edit:

| Change needed | File |
|---|---|
| Reprocessing filters | `reprocess_raw_events.py` |
| Raw row reconstruction | `reprocess_raw_events.py` |
| Replace semantics | `processing_service.py`, `reprocess_raw_events.py` |
| Reprocessing tests | `tests/test_processing_service.py` |

---

## 16. Logging

Logging is configured in `main._setup_logging()` and reused by `serial_main.py`.

Current log files:

```text
logs/app.log
logs/system_health.log
logs/parsing_errors.log
```

Behavior:

- Console handler logs INFO+.
- `app.log` logs INFO+.
- `system_health.log` logs WARNING+.
- `parsing_errors.log` is lazily configured inside `parser_engine.py`.
- Noisy libraries `telethon` and `aiosqlite` are set to WARNING.

Where to edit:

| Change needed | File |
|---|---|
| Global logging format/handlers | `main.py` |
| Parse-miss logging | `parser_engine.py` |
| Queue pressure logging | `telegram_client.py`, `serial_client.py` |
| Processing failure logging | `processing_service.py`, `persistence.py` |

---

## 17. Tests

The test suite uses:

```text
pytest
pytest-asyncio
freezegun
```

Run:

```bash
pytest tests/ -v
```

### Test fixtures

`tests/conftest.py` provides:

```python
workspace_tmp_dir()
parsed_db_path()
make_station_db()
```

`make_station_db()` creates temporary `estaciones.db` files for tests. It can create a valid schema with `station_id`, or intentionally omit `station_id` for validation tests.

### Known test files and what they cover

#### `tests/test_parser.py`

Covers:

- Tier 1 Telegram regex.
- Single-digit hour parsing.
- Missing time rejection.
- Tier 2 `MENSAJE` parsing with `canal N` and `CH-N`.
- `parse_telegram()` success and garbage rejection.

#### `tests/test_schedule.py`

Covers:

- Exact report-window start.
- Inside-window times.
- Inclusive `HH:47:00` end boundary.
- Exclusive `HH:47:01` after-window boundary.
- Exclusive `HH:44:59` before-window boundary.
- Top-of-hour times not considered tone.
- Windows sorted.

#### `tests/test_serial_parser.py`

Covers:

- SAME/EAS RWT header parsing.
- Multiple area code parsing.
- Julian day/hour/minute extraction.
- Transmitter code extraction.
- Repeated identical header counting.

#### `tests/test_station_manager.py`

Covers:

- Valid station DB load.
- Required `station_id` column.
- Duplicate `station_id` rejection.

#### `tests/test_normalization.py`

Covers:

- Telegram off-schedule close event keeps calendar report date but has no slot.
- Serial off-schedule RWT keeps calendar report date but has no slot.
- Ambiguous Telegram phone without hint stores `station_id=None`.
- Legacy station hint alias `TEUTLI` resolves to `Teuhtli`.
- Serial invalid timestamp fields raise `ValueError`.
- Serial transmitter map resolves renamed `Teuhtli`.

#### `tests/test_resolution_engine.py`

Covers:

- `resolve_slot()` prefers `OPEN` when both valid `OPEN` and `CLOSE` exist in the same slot.

#### `tests/test_telegram_client.py`

Covers:

- Live handler registration happens before catch-up.
- Restart catch-up uses watermark plus configured overlap.
- No-watermark bootstrap replays full history.
- Launch overlap override expands but does not shrink config overlap.
- No-watermark bootstrap ignores launch overlap override.
- `catchup_last_days` ignores watermark and replays recent messages in chronological order.

#### `tests/test_serial_client.py`

Covers:

- Thread-to-async queue enqueue waits for capacity.
- Enqueue exits cleanly when shutdown starts with a full queue.

#### `tests/test_processing_service.py`

Covers:

- Raw event is preserved when mid-pipeline failure occurs.
- Reprocessing produces the same normalized result as live processing.
- Corrupted `transport_meta_json` is tolerated.
- Reprocessing failure updates existing raw status without deleting existing normalized rows when delete fails.
- `retry_on_locked()` does not retry/sleep when a shared DB connection is supplied.

---

## 18. Dependency and Tooling Map

### `requirements.txt`

Runtime dependencies:

```text
telethon
pydantic
aiosqlite
python-dotenv
pyserial
tzdata
```

Test dependencies:

```text
pytest
pytest-asyncio
freezegun
```

### `pyrightconfig.json`

Configured for:

```text
venvPath = "."
venv = ".venv"
pythonVersion = "3.13"
include = ["."]
exclude = ["**/__pycache__", ".venv"]
reportMissingImports = true
```

When adding new files, keep imports compatible with Python 3.13 unless the project policy changes.

### `run_last_week.bat`

Windows convenience wrapper:

```bat
.\.venv\Scripts\python.exe main.py --telegram-catchup-last-days 7 %*
```

Use this only for Telegram ingestion replay of the last seven days. It is not a serial launcher.

---

## 19. “Where Do I Edit?” Routing Table

Use this table before searching the repo.

| Task | Start here | Also inspect |
|---|---|---|
| Add/change Telegram message format | `parser_engine.py` | `tests/test_parser.py`, `normalization.py` |
| Add/change SAME/EAS serial header format | `serial_parser.py` | `tests/test_serial_parser.py`, `normalization.py` |
| Change Telegram catch-up behavior | `telegram_client.py` | `main.py`, `tests/test_telegram_client.py`, `persistence.py` |
| Change serial COM behavior | `serial_client.py` | `serial_main.py`, `tests/test_serial_client.py`, `config.py` |
| Change report windows | `config.py`, `schedule_engine.py` | `tests/test_schedule.py`, `normalization.py` |
| Change station DB schema | `station_manager.py` | `tests/test_station_manager.py`, `tests/conftest.py` |
| Change station ambiguity behavior | `station_resolver.py` | `normalization.py`, `tests/test_normalization.py` |
| Add transmitter code | `station_resolver.py` | `tests/test_normalization.py` |
| Change event classification | `normalization.py` | `parser_engine.py`, `serial_parser.py`, tests |
| Change raw/normalized schema | `persistence.py` | `processing_service.py`, DB tests |
| Change dedupe/idempotency | `persistence.py` | `processing_service.py`, `telegram_client.py` |
| Change slot winner selection | `resolution_engine.py` | `tests/test_resolution_engine.py`, `normalization.py` |
| Change reprocessing behavior | `reprocess_raw_events.py` | `processing_service.py`, `persistence.py` |
| Change logging | `main.py` | `parser_engine.py`, ingress files |
| Add new source type | `models.py` | New ingress, `processing_service.py`, `normalization.py`, `persistence.py`, tests |

---

## 20. Core Invariants for AI Agents

These are rules an AI coding agent should not break unless explicitly asked.

### Raw data preservation

Every source message/frame should be preserved in `raw_events` before or during parsing. If parsing fails, the raw payload should still be available for audit and future reprocessing.

### Source convergence

Telegram and serial are source-specific at the edges, but both must converge into:

```text
RawEvent -> ProcessingService -> NormalizedEvent -> persistence
```

Do not create a parallel pipeline unless intentionally refactoring the architecture.

### Use internal event time for classification

Telegram tone classification uses the timestamp inside the Telegram message text, not the time the app received the Telegram message.

Serial event time is derived from SAME/EAS Julian day + hour/minute, with year inferred from `received_at.year`.

### Station ID can be null

`station_id=None` is valid for unknown or ambiguous stations. Do not force a station ID when resolution confidence is intentionally below threshold.

### Report windows are forward-only

Do not implement ±2 minute logic unless explicitly requested. The current code uses a forward-only window from `HH:45:00` through `HH:47:00` inclusive.

### Schedule logic belongs in `schedule_engine.py`

Do not duplicate window calculations in normalization, reporting, or persistence.

### Persistence owns schema

All table definitions and migrations belong in `persistence.py`.

### ProcessingService owns transactions

Do not move hot-path transaction logic into `orchestrator.py`, parser files, or ingress files.

### Shared DB transaction rule

When a function is passed an existing `db` connection, it should not independently retry with sleeps inside that transaction. This avoids inconsistent nested retry behavior.

### Reprocessing should match live processing

Historical reprocessing should use the same normalization logic as live ingestion. If live and replay diverge, fix the shared `ProcessingService` path.

---

## 21. Known Design Risks / Fragile Areas

### `persistence.py` is large and high-risk

It owns schema, migration, dedupe, projection sync, state pairing, and query helpers. Small changes can affect runtime ingestion and historical reporting.

Before modifying it:

1. Identify whether the change is schema, query, projection, or migration.
2. Add or update tests.
3. Use temporary SQLite DBs in tests.
4. Verify reprocessing behavior.

### `processing_service.py` is the hot path

Any bug here can silently affect every event. Be careful with transaction boundaries, `commit`, `rollback`, and `replace=True` behavior.

### Station ambiguity is intentional

Do not “fix” ambiguous phone behavior by picking the first station. The code intentionally stores `station_id=NULL` for unresolved ambiguity.

### Telegram catch-up must avoid gaps

Live handler registration before catch-up is intentional. Changing order can miss messages during startup.

### Serial thread-to-async queue bridge is subtle

`SerialIngress` reads serial data in worker threads and enqueues into an asyncio queue. Blocking behavior and shutdown behavior are tested. Be careful when changing `_enqueue_blocking()`.

### Timezone behavior affects reports

`LOCAL_TIMEZONE` and `tzdata` matter. Normalization stores local and UTC values differently depending on source.

### The README may lag behind code

The current code has both Telegram and serial ingestion, `ProcessingService`, reprocessing, and state/slot logic. Do not rely only on README diagrams if they appear simplified.

---

## 22. Recommended Workflow for Codex / AI Agents

When given a task:

1. Classify the task:
   - Ingestion?
   - Parsing?
   - Normalization?
   - Station resolution?
   - Schedule logic?
   - Persistence/schema?
   - Slot resolution?
   - Reprocessing?
   - Tests only?

2. Use the routing table in this file.

3. Read only the relevant module and its tests first.

4. Before editing, identify invariants touched by the change.

5. Make the smallest coherent change.

6. Add or update tests in the matching test file.

7. Run targeted tests first, then full tests:

```bash
pytest tests/test_parser.py -v
pytest tests/test_schedule.py -v
pytest tests/test_normalization.py -v
pytest tests/test_processing_service.py -v
pytest tests/ -v
```

8. When changing persistence, include a temporary DB test.

9. When changing parser or normalization, include at least one realistic raw input example.

10. When changing report-window logic, include boundary tests.

---

## 23. Suggested Future Improvements

These are not required for normal changes, but they would make the repo easier for both humans and AI agents.

### Split persistence by responsibility

`persistence.py` could eventually be split into:

```text
persistence/connection.py
persistence/schema.py
persistence/raw_events.py
persistence/normalized_events.py
persistence/projections.py
persistence/state_pairing.py
persistence/queries.py
```

This would reduce risk and make AI-targeted edits more precise.

### Add explicit architecture tests

A small test could verify that both Telegram and serial ingestion paths converge through `ProcessingService`.

### Add example raw fixtures

Add fixtures for:

```text
Telegram OPEN
Telegram CLOSE
Telegram MENSAJE/RWT
Serial RWT
Serial EQW
Serial overflow
Unknown station
Ambiguous station
```

### Add `AGENTS.md`

`AI_CONTEXT.md` is the detailed map. A shorter `AGENTS.md` at repo root could instruct Codex to read this file first.

Suggested `AGENTS.md`:

```md
# AGENTS.md

Before editing this repository, read `AI_CONTEXT.md`.

The hot path is:

RawEvent -> Orchestrator -> ProcessingService -> parser/normalization -> persistence -> resolution_engine

Do not bypass `ProcessingService` for live ingestion or reprocessing.

Use the routing table in `AI_CONTEXT.md` to locate the correct file before searching the whole repo.

Run relevant tests before finalizing changes:

```bash
pytest tests/ -v
```
```

---

## 24. Quick Glossary

### RawEvent

Source-preserved event object. Created by Telegram or serial ingress.

### NormalizedEvent

Unified event object written to `normalized_events`.

### Tono

A scheduled report/test signal considered valid only inside the configured forward-only report window.

### Report slot

The scheduled `HH:45` label for a valid report window, for example `05:45`.

### `report_date`

Calendar date of the event in local operational time.

### `is_valid_report`

True only when an event is inside a report window and has a resolved `station_id`.

### `resolved_report_slots`

Projection table containing one effective event per station/date/slot.

### `pruebas`

Projection table for off-schedule non-alert events.

### `alertas`

Projection table for off-schedule `EQW` alert events.

### `station_id`

Durable business key from `estaciones.db`. Must be preserved across station data edits.

### `source_event_id`

Source-provided or source-generated ID. Telegram uses Telegram message ID. Serial uses trace-like IDs.

### `raw_hash`

Hash used for raw-event deduplication.

---

## 25. Minimal File Map

```text
.env.example              Environment variable template
.gitignore                Git ignore rules
README.md                 Human-facing overview and setup
AI_CONTEXT.md             This AI navigation guide, if added
config.py                 Central constants and env loading
main.py                   Telegram service entry point
serial_main.py            Serial service entry point
models.py                 Pydantic/enums data model layer
telegram_client.py        Telegram ingress producer
serial_client.py          Serial COM ingress producer
orchestrator.py           Queue consumer wrapper
processing_service.py     Transactional processing core
parser_engine.py          Telegram parser
serial_parser.py          SAME/EAS parser
normalization.py          Source-specific parsed data -> NormalizedEvent
station_manager.py        estaciones.db cache and lookup
station_resolver.py       Station identity resolution
schedule_engine.py        Tono/report-window logic
persistence.py            SQLite schema, migrations, writes, queries
resolution_engine.py      resolved_report_slots winner logic
reprocess_raw_events.py   Historical reprocessing CLI
requirements.txt          Python dependencies
pyrightconfig.json        Pyright configuration
run_last_week.bat         Windows helper for 7-day Telegram replay
tests/conftest.py         Test fixtures
tests/test_parser.py      Telegram parser tests
tests/test_schedule.py    Schedule tests
tests/test_serial_parser.py Serial parser tests
tests/test_station_manager.py Station DB tests
tests/test_normalization.py Normalization/station resolution tests
tests/test_resolution_engine.py Slot resolution tests
tests/test_telegram_client.py Telegram catch-up tests
tests/test_serial_client.py Serial queue bridge tests
tests/test_processing_service.py Transaction/reprocessing tests
```

---

## 26. Final Notes for Future Agents

This repo is small enough to understand quickly but has several high-consequence seams:

- Time classification.
- Station identity.
- Raw-event preservation.
- Deduplication.
- Reprocessing.
- SQLite transaction boundaries.

Most changes should touch one subsystem plus its tests. If a task seems to require editing many unrelated files, pause and re-check whether the change belongs in `ProcessingService`, `normalization`, or `persistence` instead.

