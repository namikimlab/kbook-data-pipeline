-- raw payloads (bronze)
create table if not exists public.raw_nl_books (
  id bigserial primary key,
  fetched_at timestamptz not null default now(),
  page_no int,
  source_record jsonb not null,
  rec_hash text generated always as (md5(source_record::text)) stored,
  unique(rec_hash)
);

-- forward sync cursor
create table if not exists public.sync_state (
  id bigserial primary key,
  job_name text unique not null,                  -- e.g., 'nl_forward'
  last_input_date_synced date,                    -- latest INPUT_DATE you’ve seen
  last_update_sweep_through date,                 -- last day you re-checked updates
  last_run_at timestamptz default now()
);
