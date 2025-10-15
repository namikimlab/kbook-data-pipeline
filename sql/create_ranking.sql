-- DAILY
create table if not exists public.book_rank_daily (
  day date not null,
  isbn13 text not null check (isbn13 ~ '^[0-9]{13}$'),
  likes_total bigint not null,
  rank int not null,
  primary key (day, isbn13)
);

create index if not exists idx_book_rank_daily_day_rank
  on public.book_rank_daily (day, rank);

-- WEEKLY (ISO week: date_trunc('week') starts Monday)
create table if not exists public.book_rank_weekly (
  week_start date not null,  -- Monday of the ISO week
  isbn13 text not null check (isbn13 ~ '^[0-9]{13}$'),
  likes_total bigint not null,
  rank int not null,
  primary key (week_start, isbn13)
);

create index if not exists idx_book_rank_weekly_week_rank
  on public.book_rank_weekly (week_start, rank);

-- MONTHLY
create table if not exists public.book_rank_monthly (
  month_start date not null, -- first day of the month
  isbn13 text not null check (isbn13 ~ '^[0-9]{13}$'),
  likes_total bigint not null,
  rank int not null,
  primary key (month_start, isbn13)
);

create index if not exists idx_book_rank_monthly_month_rank
  on public.book_rank_monthly (month_start, rank);


-- snapshot inserts
-- Daily (run once per day, after your ingest + MV refresh)
insert into public.book_rank_daily (day, isbn13, likes_total, rank)
select
  current_date as day,
  c.isbn13,
  c.likes_count as likes_total,
  dense_rank() over (order by c.likes_count desc) as rank
from public.books_like_counters c
where c.likes_count > 0
on conflict (day, isbn13) do update
set likes_total = excluded.likes_total,
    rank        = excluded.rank;

-- Weekly (run once per week; ISO week Monday recommended)
with params as (
  select date_trunc('week', now())::date as week_start
)
insert into public.book_rank_weekly (week_start, isbn13, likes_total, rank)
select
  p.week_start,
  c.isbn13,
  c.likes_count,
  dense_rank() over (order by c.likes_count desc)
from public.books_like_counters c
cross join params p
where c.likes_count > 0
on conflict (week_start, isbn13) do update
set likes_total = excluded.likes_total,
    rank        = excluded.rank;

-- Monthly (run once on the first day of month, or last day—your choice)
with params as (
  select date_trunc('month', now())::date as month_start
)
insert into public.book_rank_monthly (month_start, isbn13, likes_total, rank)
select
  p.month_start,
  c.isbn13,
  c.likes_count,
  dense_rank() over (order by c.likes_count desc)
from public.books_like_counters c
cross join params p
where c.likes_count > 0
on conflict (month_start, isbn13) do update
set likes_total = excluded.likes_total,
    rank        = excluded.rank;

-- Optional: convenience views for “top N for a period”
-- Top daily for a given :d
create or replace view public.top_books_daily as
select d.day, d.isbn13, d.likes_total, d.rank, bp.title, bp.author, bp.publisher
from public.book_rank_daily d
left join public.books_public bp using (isbn13);

-- Top weekly for a given :ws
create or replace view public.top_books_weekly as
select w.week_start, w.isbn13, w.likes_total, w.rank, bp.title, bp.author, bp.publisher
from public.book_rank_weekly w
left join public.books_public bp using (isbn13);

-- Top monthly for a given :ms
create or replace view public.top_books_monthly as
select
  m.month_start,
  m.isbn13,
  m.likes_total,
  m.rank,
  coalesce(bp.title,     '알수 없음')   as title,
  coalesce(bp.author,    '알수 없음')  as author,
  coalesce(bp.publisher, '알수 없음') as publisher
from public.book_rank_monthly m
left join public.books_public bp using (isbn13);

-- Index & constraints (performance + data quality)

-- book_rank_monthly: speed "top N for a month" queries
create index if not exists idx_book_rank_monthly_month_rank
  on public.book_rank_monthly (month_start, rank);

-- (optional) if you often fetch by month ordered by likes
create index if not exists idx_book_rank_monthly_month_likes
  on public.book_rank_monthly (month_start, likes_total desc);

-- books_public:
-- If books_public is a MATERIALIZED VIEW, you can index it:
-- (run after each materialized view refresh is set up)
create unique index if not exists uq_books_public_isbn13
  on public.books_public (isbn13);

-- If books_public is a plain VIEW, you can't index a view.
-- In that case, enforce uniqueness in the underlying silver table/materialized view instead.


-- 3) Permissions & RLS (so your site can read it)
grant usage on schema public to anon, authenticated;
grant select on public.top_books_monthly to anon, authenticated;

-- Enable RLS if not already
alter table public.book_rank_monthly enable row level security;

-- Allow read for everyone (adjust as needed)
create policy r_book_rank_monthly_public
on public.book_rank_monthly
for select
to anon, authenticated
using (true);

-- For a MATERIALIZED VIEW: RLS does not apply directly to MVs in Postgres,
-- but your MV selects from base tables at refresh time.
-- If your top_books_monthly view selects directly from the MV (books_public),
-- and you’ve granted SELECT on the MV, you’re fine.
grant select on public.books_public to anon, authenticated;