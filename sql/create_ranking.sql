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
select m.month_start, m.isbn13, m.likes_total, m.rank, bp.title, bp.author, bp.publisher
from public.book_rank_monthly m
left join public.books_public bp using (isbn13);
