#!/usr/bin/env bash
# assignment1.sh
# CSE 511 – Assignment 1: Database Creation & Loading (Pushshift Reddit subset)
# Loads CSVs, creates tables in lowercase, and adds constraints after data load.
# Does NOT create a database or change DB encoding (per assignment rules).

set -euo pipefail

############################
# Config (override as env) #
############################
: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=5432}"
: "${PGDATABASE:=postgres}"
: "${PGUSER:=postgres}"
: "${PGPASSWORD:=postgres}"   # exporter/grader user (ok per PDF)
export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD

# CSV paths (must be in same folder as this script, per PDF)
AUTHORS_CSV="./authors.csv"
SUBREDDITS_CSV="./subreddits.csv"
SUBMISSIONS_CSV="./submissions.csv"
COMMENTS_CSV="./comments.csv"

for f in "$AUTHORS_CSV" "$SUBREDDITS_CSV" "$SUBMISSIONS_CSV" "$COMMENTS_CSV"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: Required CSV not found: $f"
    echo "Place authors.csv, subreddits.csv, submissions.csv, comments.csv next to assignment1.sh."
    exit 1
  fi
done

########################################
# Create SQL files (no DB creation!)   #
########################################

# 01_create_tables.sql: final + staging tables (no FKs yet)
cat > 01_create_tables.sql <<'SQL'
-- 01_create_tables.sql
-- Final tables (no foreign keys yet for faster bulk load), all lowercase names.

-- Drop if exist (idempotent)
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS submissions CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS subreddits CASCADE;

DROP TABLE IF EXISTS staging_comments;
DROP TABLE IF EXISTS staging_submissions;
DROP TABLE IF EXISTS staging_authors;
DROP TABLE IF EXISTS staging_subreddits;

-- Final: authors
CREATE TABLE authors (
  id                text PRIMARY KEY,          -- e.g., t2_*
  retrieved_on      bigint,
  name              text NOT NULL,             -- username
  created_utc       bigint,
  link_karma        integer,
  comment_karma     integer,
  profile_img       text,
  profile_color     text,
  profile_over_18   boolean
);

-- Final: subreddits
CREATE TABLE subreddits (
  id                      text PRIMARY KEY,    -- e.g., t5_*
  name                    text,                -- e.g., t5_vf2
  display_name            text,
  title                   text,
  description             text,
  public_description      text,
  created_utc             bigint,
  retrieved_utc           bigint,
  subscribers             integer,
  header_img              text,
  banner_background_image text,
  subreddit_type          text,
  whitelist_status        text,
  hide_ads                boolean,
  over_18                 boolean
);

-- Final: submissions
CREATE TABLE submissions (
  id            text PRIMARY KEY,              -- e.g., iemqy
  name          text UNIQUE,                   -- e.g., t3_iemqy
  title         text,
  url           text,
  permalink     text,
  author        text,                          -- FK added later -> authors(name)
  subreddit_id  text NOT NULL,                 -- FK added later -> subreddits(id)
  created_utc   bigint,
  ups           integer,
  downs         integer,
  num_comments  integer,
  edited        boolean,
  likes         text,
  num_reports   integer
);

-- Final: comments
CREATE TABLE comments (
  id                       text PRIMARY KEY,   -- e.g., c22x4aq
  name                     text UNIQUE,        -- e.g., t1_c22x4aq
  body                     text,
  author                   text,               -- FK later -> authors(name)
  subreddit_id             text,               -- FK later -> subreddits(id)
  subreddit                text,               -- subreddit display text (redundant string)
  link_id                  text NOT NULL,      -- FK later -> submissions(name), e.g., t3_*
  parent_id                text,               -- can be t1_* (comment) or t3_* (submission), so no FK
  created_utc              bigint,
  retrieved_on             bigint,
  ups                      integer,
  downs                    integer,
  score                    integer,
  distinguished            text,
  controversiality         integer,
  edited                   boolean,
  gilded                   integer,
  author_flair_css_class   text,
  author_flair_text        text,
  score_hidden             boolean,
  archived                 boolean
);

-- Staging tables: raw TEXT for CSV ingest (header-driven), one-to-one with CSV columns
CREATE TABLE staging_submissions (
  downs text, url text, id text, edited text, num_reports text,
  created_utc text, name text, title text, author text, permalink text,
  num_comments text, likes text, subreddit_id text, ups text
);

CREATE TABLE staging_comments (
  distinguished text, downs text, created_utc text, controversiality text, edited text,
  gilded text, author_flair_css_class text, id text, author text, retrieved_on text,
  score_hidden text, subreddit_id text, score text, name text, author_flair_text text,
  link_id text, archived text, ups text, parent_id text, subreddit text, body text
);

CREATE TABLE staging_authors (
  id text, retrieved_on text, name text, created_utc text, link_karma text,
  comment_karma text, profile_img text, profile_color text, profile_over_18 text
);

CREATE TABLE staging_subreddits (
  banner_background_image text, created_utc text, description text, display_name text,
  header_img text, hide_ads text, id text, over_18 text, public_description text,
  retrieved_utc text, name text, subreddit_type text, subscribers text, title text,
  whitelist_status text
);
SQL

# 02_copy_to_staging.sql: use \copy for speed, treat empty strings as NULLs
cat > 02_copy_to_staging.sql <<'SQL'
-- 02_copy_to_staging.sql
-- Fast CSV import to staging tables. CSVs must be in the same folder as assignment1.sh.
\set ON_ERROR_STOP on

-- Improve ingest perf (session-local, allowed)
SET synchronous_commit = OFF;
SET maintenance_work_mem = '1GB';
SET work_mem = '128MB';
SET temp_buffers = '256MB';

-- Import (CSV HEADER). Interpret empty fields as NULL to ease casting.
\copy staging_authors     FROM './authors.csv'     WITH (FORMAT csv, HEADER true, NULL '');
\copy staging_subreddits  FROM './subreddits.csv'  WITH (FORMAT csv, HEADER true, NULL '');
\copy staging_submissions FROM './submissions.csv' WITH (FORMAT csv, HEADER true, NULL '');
\copy staging_comments    FROM './comments.csv'    WITH (FORMAT csv, HEADER true, NULL '');
SQL

# 03_transform_load.sql: cast/clean into final tables
cat > 03_transform_load.sql <<'SQL'
-- 03_transform_load.sql
\set ON_ERROR_STOP on

-- Helper: bool from text 'true'/'false'
-- We’ll inline cast with (lower(col) = 'true') where appropriate.

-- AUTHORS
INSERT INTO authors (
  id, retrieved_on, name, created_utc, link_karma, comment_karma,
  profile_img, profile_color, profile_over_18
)
SELECT
  id,
  NULLIF(retrieved_on,'')::bigint,
  name,
  NULLIF(created_utc,'')::bigint,
  NULLIF(link_karma,'')::integer,
  NULLIF(comment_karma,'')::integer,
  profile_img,
  profile_color,
  CASE WHEN lower(coalesce(profile_over_18,''))='true' THEN true
       WHEN lower(coalesce(profile_over_18,''))='false' THEN false
       ELSE NULL END
FROM staging_authors
WHERE id IS NOT NULL;

-- SUBREDDITS
INSERT INTO subreddits (
  id, name, display_name, title, description, public_description,
  created_utc, retrieved_utc, subscribers, header_img, banner_background_image,
  subreddit_type, whitelist_status, hide_ads, over_18
)
SELECT
  id,
  name,
  display_name,
  title,
  description,
  public_description,
  NULLIF(created_utc,'')::bigint,
  NULLIF(retrieved_utc,'')::bigint,
  NULLIF(subscribers,'')::integer,
  header_img,
  banner_background_image,
  subreddit_type,
  whitelist_status,
  CASE WHEN lower(coalesce(hide_ads,''))='true' THEN true
       WHEN lower(coalesce(hide_ads,''))='false' THEN false
       ELSE NULL END,
  CASE WHEN lower(coalesce(over_18,''))='true' THEN true
       WHEN lower(coalesce(over_18,''))='false' THEN false
       ELSE NULL END
FROM staging_subreddits
WHERE id IS NOT NULL;

-- SUBMISSIONS
INSERT INTO submissions (
  id, name, title, url, permalink, author, subreddit_id,
  created_utc, ups, downs, num_comments, edited, likes, num_reports
)
SELECT
  id,
  name,
  title,
  url,
  permalink,
  NULLIF(author,''),
  subreddit_id,
  NULLIF(created_utc,'')::bigint,
  NULLIF(ups,'')::integer,
  NULLIF(downs,'')::integer,
  NULLIF(num_comments,'')::integer,
  CASE WHEN lower(coalesce(edited,''))='true' THEN true
       WHEN lower(coalesce(edited,''))='false' THEN false
       ELSE NULL END,
  likes,
  NULLIF(num_reports,'')::integer
FROM staging_submissions
WHERE id IS NOT NULL;

-- COMMENTS
INSERT INTO comments (
  id, name, body, author, subreddit_id, subreddit, link_id, parent_id,
  created_utc, retrieved_on, ups, downs, score, distinguished, controversiality,
  edited, gilded, author_flair_css_class, author_flair_text, score_hidden, archived
)
SELECT
  id,
  name,
  body,
  NULLIF(author,''),
  subreddit_id,
  subreddit,
  link_id,
  parent_id,
  NULLIF(created_utc,'')::bigint,
  NULLIF(retrieved_on,'')::bigint,
  NULLIF(ups,'')::integer,
  NULLIF(downs,'')::integer,
  NULLIF(score,'')::integer,
  distinguished,
  NULLIF(controversiality,'')::integer,
  CASE WHEN lower(coalesce(edited,''))='true' THEN true
       WHEN lower(coalesce(edited,''))='false' THEN false
       ELSE NULL END,
  NULLIF(gilded,'')::integer,
  author_flair_css_class,
  author_flair_text,
  CASE WHEN lower(coalesce(score_hidden,''))='true' THEN true
       WHEN lower(coalesce(score_hidden,''))='false' THEN false
       ELSE NULL END,
  CASE WHEN lower(coalesce(archived,''))='true' THEN true
       WHEN lower(coalesce(archived,''))='false' THEN false
       ELSE NULL END
FROM staging_comments
WHERE id IS NOT NULL;
SQL

# 04_constraints_indexes.sql: add FKs & helpful indexes after load
cat > 04_constraints_indexes.sql <<'SQL'
-- 04_constraints_indexes.sql
\set ON_ERROR_STOP on

-- Unique constraints to support FKs by "name" where needed
ALTER TABLE authors ADD CONSTRAINT uq_authors_name UNIQUE (name);
ALTER TABLE submissions ADD CONSTRAINT uq_submissions_name UNIQUE (name);
ALTER TABLE comments   ADD CONSTRAINT uq_comments_name UNIQUE (name);
ALTER TABLE subreddits ADD CONSTRAINT uq_subreddits_name UNIQUE (name);

-- Foreign keys (added after data load for performance)
ALTER TABLE submissions
  ADD CONSTRAINT fk_submissions_subreddit
  FOREIGN KEY (subreddit_id) REFERENCES subreddits(id)
  ON UPDATE CASCADE ON DELETE RESTRICT;

-- Many authors/usernames may be missing or "[deleted]"; allow NULL, set NULL on delete.
ALTER TABLE submissions
  ADD CONSTRAINT fk_submissions_author
  FOREIGN KEY (author) REFERENCES authors(name)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_subreddit
  FOREIGN KEY (subreddit_id) REFERENCES subreddits(id)
  ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_author
  FOREIGN KEY (author) REFERENCES authors(name)
  ON UPDATE CASCADE ON DELETE SET NULL;

-- link_id uses the submission "name" (e.g., t3_*), not the base36 "id"
ALTER TABLE comments
  ADD CONSTRAINT fk_comments_link_submission
  FOREIGN KEY (link_id) REFERENCES submissions(name)
  ON UPDATE CASCADE ON DELETE CASCADE;

-- Helpful indexes (FKs auto-index parents; we add on join/filter cols)
CREATE INDEX IF NOT EXISTS idx_submissions_subreddit_id ON submissions(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_submissions_author      ON submissions(author);
CREATE INDEX IF NOT EXISTS idx_comments_subreddit_id   ON comments(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_comments_author         ON comments(author);
CREATE INDEX IF NOT EXISTS idx_comments_parent_id      ON comments(parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_link_id        ON comments(link_id);

-- Analyze for planner stats
ANALYZE authors;
ANALYZE subreddits;
ANALYZE submissions;
ANALYZE comments;
SQL

#################################
# Execute: build & load & link  #
#################################

echo "==> Creating tables…"
psql -v ON_ERROR_STOP=1 -f 01_create_tables.sql

echo "==> Copying CSVs into staging (fast \copy)…"
psql -v ON_ERROR_STOP=1 -f 02_copy_to_staging.sql

echo "==> Transforming & loading into final tables…"
psql -v ON_ERROR_STOP=1 -f 03_transform_load.sql

echo "==> Adding constraints & indexes…"
psql -v ON_ERROR_STOP=1 -f 04_constraints_indexes.sql

echo "==> Done. Loaded data and applied constraints successfully."

