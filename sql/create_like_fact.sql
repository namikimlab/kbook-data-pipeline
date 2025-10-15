-- FACT TABLE: one active like per (user_id, isbn13)
create table if not exists public.book_likes (
  user_id uuid not null
    references auth.users (id)
    on delete cascade,
    
  isbn13 text not null
    check (isbn13 ~ '^[0-9]{13}$'),   -- ensure valid 13-digit ISBN

  active boolean not null default true,   -- true = like, false = unlike

  created_at timestamptz not null default now(),

  -- Prevent duplicate likes per user/book
  primary key (user_id, isbn13)
);

-- Helpful indexes
create index if not exists idx_book_likes_isbn_active
  on public.book_likes(isbn13)
  where active;

create index if not exists idx_book_likes_user_active
  on public.book_likes(user_id)
  where active;

-- Enable Row Level Security (RLS)
alter table public.book_likes enable row level security;

-- Allow authenticated users to read likes (optional; can be restricted)
create policy likes_select_authenticated
on public.book_likes
for select
to authenticated
using (true);

-- Allow only the owner to insert/update/delete their own likes
create policy likes_mutate_owner
on public.book_likes
for all
to authenticated
using (user_id = auth.uid())
with check (user_id = auth.uid());


-- test 
-- simulate a user liking a book
insert into public.book_likes (user_id, isbn13)
values ('00000000-0000-0000-0000-000000000001', '9788983927753');

-- query all likes
select * from public.book_likes;

-- unlike (toggle off)
update public.book_likes
set active = false
where user_id = '00000000-0000-0000-0000-000000000001'
  and isbn13 = '9788983927753';
