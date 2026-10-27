# Shipping Technology Data Platform

A small data platform that ingests raw GPS logs from two cargo vessels, validates and cleans them, joins them with weather data along their route, persists everything in Postgres, and serves a handful of metrics through a FastAPI service. Built as a take-home assessment for a Data Platform Engineer role.

## Stack

- Python 3.11
- pandas (parsing/cleaning)
- SQLAlchemy 2.0 + Postgres 16
- FastAPI + Pydantic (API layer)
- pytest (validation logic is unit tested)
- Docker Compose (single-command reproduction)

All dependency versions are pinned in `requirements.txt`.

## How it fits together

```
raw_messages_clean.csv  ─┐
                          ├──▶  ingest.py  ──▶  Postgres  ──▶  FastAPI
weather_data.json       ─┘         │                              │
                                    ▼                              ▼
                          rejected_messages table          /health
                          (nothing silently dropped)        /metrics/average-speed
                                                              /metrics/wind-speed
```

Every row from the ship logs gets checked before it's allowed anywhere near the database. If a row is broken, it doesn't get thrown away. It gets stored in its own table with a clear reason why it failed, so nothing ever disappears without a trace.

## Getting it running

You genuinely just need Docker and an internet connection. That's it.

```bash
docker compose up --build
```

Give it a few minutes on the first run, since it needs to pull images and install everything fresh. Once it settles, open your browser to:

```
http://localhost:8000/docs
```

You'll see the three endpoints, live and callable right from the page. Under the hood, the database spins up first, the ingest job runs once to load all the data, and then the API comes online. You don't have to do any of that by hand.

## Endpoints

### `GET /health`
```json
{"status": "ok"}
```

### `GET /metrics/average-speed?date=2019-02-13`
Hourly average speed per ship, for that UTC day. Only positions flagged reliable (`status='A'`) are counted; readings flagged as a navigation warning are stored but excluded, so a shaky GPS fix can't skew the number.

```json
{
  "date": "2019-02-13",
  "results": [
    {"device_id": "st-1a2090", "hour": 12, "avg_speed_knots": 11.06},
    {"device_id": "st-1a2090", "hour": 13, "avg_speed_knots": 8.89}
  ]
}
```

### `GET /metrics/wind-speed?device_id=st-1a2090`
Daily min and max wind speed near that ship's route, in metres per second.

```json
{
  "device_id": "st-1a2090",
  "results": [
    {"day": "2019-02-13", "min_wind_spd_ms": 1.0, "max_wind_spd_ms": 7.0}
  ]
}
```

Bad input on either query param returns a clean `422` with a proper validation message, not a stack trace. Full interactive docs (and a place to try these yourself) live at `/docs`.

## Data model

**`ship_positions`** — the clean layer. One row per validated GPS message: `device_id` (text, keeps leading zeros like `0001`), `ts_utc`, `status` (`A`/`V`), signed `lat`/`lon` (south and west are negative), `speed_knots`, `true_course`, `mag_variation`, and `row_hash`, a sha256 fingerprint of the original 15 raw fields. `row_hash` is unique, so re-running ingestion never inserts the same row twice.

**`rejected_messages`** — the quarantine. Every row that fails validation lands here with a plain-English `reject_reason` instead of being dropped. Nothing disappears silently.

**`weather_readings`** — one row per hourly weather reading, tagged with `city_name`, the source block's `block_lat`/`block_lon`, `wind_spd_ms`, and `nearest_device_id` (the ship it was matched against), so metric (b) is a straight `GROUP BY`.

## Running it end to end

Real numbers from the last clean run, tearing everything down first:

```
docker compose down -v
docker compose up --build
```

```
positions_inserted: 29052
positions_skipped: 0
rejected: 0
weather_rows: 384
weather_skipped: 0
```

Then open `http://localhost:8000/docs`.

## Running the tests

```
pytest tests/test_validation.py -v
```

45 tests, all passing: a clean row parses with correct sign folding and a stable hash, every rejection rule fires with the right reason, and missing or blank fields are caught individually across all 15 source fields.

## Assumptions

Full reasoning and alternatives considered for each of these are in `docs/decisions.md`. Short version:

1. **Time source is the `datetime` column, parsed as UTC.** The brief says unix timestamp; the actual file has text timestamps (`2019-02-13 10:15:00`). Coded against the real data and logged the mismatch. The embedded `datestamp` field is never used for time logic. It's unreliable: one device's embedded date read 2018 while everything else in the file pointed to February 2019.
2. **Coordinates are plain decimal degrees, not NMEA ddmm.mm.** Verified against the actual values (all latitudes ≤ 90) rather than assumed.
3. **Wind speed (`wind_spd`) is metres per second.** The weather JSON's shape matches the Weatherbit historical API, which reports wind in m/s. Stated as an explicit assumption, not treated as fact.
4. **Weather is joined to the nearest ship position on the same UTC day**, via haversine distance. The weather blocks' coordinates coincide with the ship's own GPS track, so this match is exact in practice. Haversine is used over flat Euclidean distance because a degree of longitude shrinks the further you are from the equator.
5. **Idempotency key is `row_hash`, not `original_message_id`.** The source IDs repeat, but no two rows are fully identical, so the ID alone can't be trusted as unique. Every row is fingerprinted (sha256 of all 15 raw fields) instead, with a unique constraint. Re-running ingestion inserts nothing new.
6. **Average speed only counts `status='A'` rows.** `V` (navigation receiver warning) rows are stored but excluded, since a device flagging its own fix as unreliable shouldn't be allowed to skew the average.
7. **`device_id` is text, not numeric.** One ship's ID is `0001`; storing it as a number silently drops the leading zeros the moment anything (like Excel) touches it.

## Shortcuts taken, and the production alternative

- **Ingestion is a single one-shot script.** Production: orchestrated incremental loads (Airflow or Dagster), schema migrations via Alembic, data quality checks as code (Great Expectations) rather than a hand-written validator.
- **Weather-to-ship matching runs as a plain Python nested loop.** Fine at this scale (27k positions × 384 readings). Production: a spatial index like PostGIS.
- **No authentication on the API.** Production: sits behind a gateway with API keys.
- **Everything runs through Docker Compose locally.** Production: managed Postgres, container orchestration, and a real deployment pipeline.
- **Rejected rows are quarantined but nothing alerts on them.** Production: a spike in rejection rate should page someone, not wait to be noticed.
- **No CI.** Tests exist and pass locally, but nothing runs them automatically on push. Production: GitHub Actions running `pytest` on every commit.

## Where the full reasoning lives

Every decision above, along with a few smaller ones, is written out properly in [`docs/decisions.md`](docs/decisions.md), including what I considered and rejected along the way. If you're curious about the why behind any of this, that's the place to look.
