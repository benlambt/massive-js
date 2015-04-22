create table %s(
  id text primary key,
  body jsonb not null,
  search tsvector
);
create index idx_%s on %s using GIN(body jsonb_path_ops);
