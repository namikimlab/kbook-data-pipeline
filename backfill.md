Great—track backfill with a tiny “state + ledger” pair. Simple, reliable, resumable.

# 1) What to track

* **Cursor**: how far you’ve gone (by `PUBLISH_PREDATE` windows).
* **Window status**: planned vs in-progress vs done vs failed.
* **Pagination**: last `page_no` reached for the current window (for resume).
* **Counts**: fetched vs upserted vs updated.

# 2) Tables (minimal DDL)

```sql
-- A. Single source of truth for all ingestion cursors
create table if not exists ingestion_state (
  id bigserial primary key,
  job_name text not null,         -- e.g., 'nl_backfill', 'nl_forward'
  cursor_type text not null,      -- 'publish_predate' | 'input_date'
  cursor_start date,              -- window start (inclusive)
  cursor_end date,                -- window end (inclusive)
  last_page_no int,               -- last completed page within window
  last_run_at timestamptz default now(),
  status text not null,           -- 'idle' | 'running' | 'done' | 'failed'
  notes text,
  unique(job_name, cursor_type, cursor_start, cursor_end)
);

-- B. Optional ledger per window (nice for dashboards & retries)
create table if not exists backfill_ledger (
  id bigserial primary key,
  window_start date not null,
  window_end date not null,
  pages_total int,
  pages_done int default 0,
  rows_fetched int default 0,
  rows_upserted int default 0,
  rows_updated int default 0,
  last_page_no int default 0,
  status text not null,           -- 'planned' | 'running' | 'done' | 'failed' | 'skipped'
  last_run_at timestamptz default now(),
  error_msg text,
  unique(window_start, window_end)
);
```

# 3) How to use it (backfill)

1. **Plan windows** (e.g., 30-day blocks going backward):

```sql
-- example: insert planned windows once
insert into backfill_ledger (window_start, window_end, status)
values
  ('2025-09-01','2025-09-30','planned'),
  ('2025-08-01','2025-08-31','planned'),
  ...;
```

2. **Pick next window**:

```sql
select * from backfill_ledger
where status in ('planned','failed')
order by window_start desc
limit 1;
```

3. **Run** with API params:

* `start_publish_date=YYYYMMDD` = `window_start`
* `end_publish_date=YYYYMMDD` = `window_end`
* Loop `page_no` from `coalesce(last_page_no,1)` to last page.

4. **Checkpoint each page**:

```sql
update backfill_ledger
set pages_done = pages_done + 1,
    last_page_no = $current_page,
    rows_fetched = rows_fetched + $fetched,
    rows_upserted = rows_upserted + $upserted,
    rows_updated  = rows_updated  + $updated,
    last_run_at = now(),
    status = 'running'
where id = $ledger_id;
```

5. **Complete window**:

```sql
update backfill_ledger
set status = 'done', last_run_at = now()
where id = $ledger_id;
```

6. **Record global cursor (optional)**:

```sql
insert into ingestion_state(job_name, cursor_type, cursor_start, cursor_end, last_page_no, status)
values ('nl_backfill','publish_predate', $start, $end, null, 'done')
on conflict (job_name, cursor_type, cursor_start, cursor_end)
do update set status='done', last_run_at=now();
```

# 4) Resume logic

* On start, read the latest `backfill_ledger` row with `status in ('running','failed')`: resume from `last_page_no + 1`.
* If none, take the newest `planned` window.
* If API errors mid-page, **do not** advance `last_page_no`; retry the same page.

# 5) Safe overlap (to avoid gaps)

* When generating windows, **overlap by 1–3 days** (e.g., previous window_end + 1 day minus overlap).
* Upserts are idempotent, so overlaps are safe.

# 6) Progress dashboards (quick queries)

* % done:

```sql
select
  count(*) filter (where status='done')::float / nullif(count(*),0) as pct_done
from backfill_ledger;
```

* Remaining windows:

```sql
select window_start, window_end
from backfill_ledger
where status in ('planned','failed','running')
order by window_start desc;
```

# 7) Forward sync state (for completeness)

Keep a simple row:

```sql
create table if not exists sync_state (
  id bigserial primary key,
  job_name text unique,                 -- 'nl_forward'
  last_input_date_synced date,          -- max INPUT_DATE seen
  last_update_sweep_through date,       -- last day you re-checked updates
  last_run_at timestamptz default now()
);
```

That’s it: a tiny, auditable ledger + a cursor. Easy to resume, easy to visualize, and safe against duplicates.
