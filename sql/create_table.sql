-- 1) Core table
create table if not exists public.books (
  source_id   text primary key,          -- maps "@id"
  title       text,                      -- "title"
  subtitle    text,                      -- "remainderOfTitle"
  authors     text[],                    -- ["creator", "dcterms:creator"] flattened to text[]
  subjects    text[],                    -- "subject" flattened to text[]
  publisher   text,                      -- "publisher"
  pub_year    int,                       -- "issuedYear" or year(issued); null if unknown
  series      text,                      -- "titleOfSeries"
  volume      text,                      -- "volumeOfSeries" or "volume"
  isbn13      text,                      -- "isbn" (may be null / multiple forms)
  extra       jsonb,                     -- keep all other keys here
  created_at  timestamptz not null default now(),
  updated_at  timestamptz not null default now()
);

-- 2) Helpful indexes (optional but recommended)
create unique index if not exists books_isbn13_uniq
  on public.books (isbn13) where isbn13 is not null;

create index if not exists books_subjects_gin on public.books using gin (subjects);
create index if not exists books_authors_gin  on public.books using gin (authors);
create index if not exists books_extra_gin    on public.books using gin (extra);

-- 3) Keep updated_at fresh on updates
create or replace function set_updated_at() returns trigger as $$
begin
  new.updated_at := now();
  return new;
end; $$ language plpgsql;

drop trigger if exists trg_books_updated_at on public.books;
create trigger trg_books_updated_at
before update on public.books
for each row execute function set_updated_at();