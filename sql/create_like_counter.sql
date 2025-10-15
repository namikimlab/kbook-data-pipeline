create table if not exists public.books_like_counters (
  isbn13 text primary key check (isbn13 ~ '^[0-9]{13}$'),
  likes_count bigint not null default 0
);

-- Triggers
-- Insert: +1 when a new like is added
create or replace function public.like_counter_after_insert()
returns trigger language plpgsql as $$
begin
  if new.active then
    insert into public.books_like_counters (isbn13, likes_count)
    values (new.isbn13, 1)
    on conflict (isbn13)
    do update set likes_count = public.books_like_counters.likes_count + 1;
  end if;
  return new;
end $$;


-- Update: handle toggling active → inactive (or vice versa)
create or replace function public.like_counter_after_update()
returns trigger language plpgsql as $$
begin
  if old.active = false and new.active = true then
    -- re-activate like → +1
    insert into public.books_like_counters (isbn13, likes_count)
    values (new.isbn13, 1)
    on conflict (isbn13)
    do update set likes_count = public.books_like_counters.likes_count + 1;
  elsif old.active = true and new.active = false then
    -- un-like → −1
    update public.books_like_counters
       set likes_count = greatest(0, likes_count - 1)
     where isbn13 = old.isbn13;
  end if;
  return new;
end $$;

-- Delete: −1 when a like row is deleted 
create or replace function public.like_counter_after_delete()
returns trigger language plpgsql as $$
begin
  if old.active then
    update public.books_like_counters
       set likes_count = greatest(0, likes_count - 1)
     where isbn13 = old.isbn13;
  end if;
  return old;
end $$;

-- Attach triggers to your book_likes table
drop trigger if exists trg_like_insert on public.book_likes;
create trigger trg_like_insert
after insert on public.book_likes
for each row execute function public.like_counter_after_insert();

drop trigger if exists trg_like_update on public.book_likes;
create trigger trg_like_update
after update on public.book_likes
for each row execute function public.like_counter_after_update();

drop trigger if exists trg_like_delete on public.book_likes;
create trigger trg_like_delete
after delete on public.book_likes
for each row execute function public.like_counter_after_delete();


-- Test
-- Like one book
insert into public.book_likes (user_id, isbn13)
values ('00000000-0000-0000-0000-000000000001', '9788983927753');

-- Check counter
select * from public.books_like_counters;

-- Unlike
update public.book_likes
   set active = false
 where user_id = '00000000-0000-0000-0000-000000000001'
   and isbn13 = '9788983927753';
