-- drop table if exists public.books cascade;

-- 확장
create extension if not exists pgcrypto;

-- 매핑에 딱 맞춘 테이블
create table if not exists public.books (
  book_id    uuid primary key default gen_random_uuid(),

  -- 매핑 컬럼들
  source_id  text not null unique,   -- "@id"
  isbn13     text unique,            -- "isbn" 정규화 후 (없으면 NULL 허용)
  title      text not null,          -- "title"
  subtitle   text,                   -- "remainderOfTitle"
  authors    text[] default '{}',    -- "creator" 또는 "dcterms:creator" → text[]
  subjects   text[] default '{}',    -- "subject" → text[]
  publisher  text,                   -- "publisher"
  pub_year   int,                    -- "issuedYear" 또는 "issued"에서 4자리 연도
  series     text,                   -- "titleOfSeries"
  volume     text,                   -- "volumeOfSeries" / "volume"
  extra      jsonb default '{}'::jsonb,  -- 매핑 안 된 모든 원본 필드

  created_at timestamptz default now(),
  updated_at timestamptz default now(),

  -- FTS (간단한 가중치)
  search_tsv tsvector generated always as (
    setweight(to_tsvector('simple', coalesce(title,'')), 'A') ||
    setweight(to_tsvector('simple', coalesce(array_to_string(authors,' '),'')), 'B') ||
    to_tsvector('simple', coalesce(array_to_string(subjects,' '), ''))
  ) stored,

  -- 가벼운 유효성 (선택)
  constraint chk_pub_year range between 1000 and 2100
    deferrable initially deferred
);

-- 인덱스
create index if not exists books_fts_gin on public.books using gin (search_tsv);
create index if not exists books_year_idx on public.books(pub_year);

-- updated_at 자동 갱신 트리거 (편의)
create or replace function public.set_updated_at() returns trigger as $$
begin
  new.updated_at = now();
  return new;
end $$ language plpgsql;

drop trigger if exists trg_books_updated_at on public.books;
create trigger trg_books_updated_at
before update on public.books
for each row execute procedure public.set_updated_at();
