#!/usr/bin/env bash
# assignment1.sh
# Assignment 1 loader using pg_bulkload (all-in-one, no extra files).
# Creates schema, loads via pg_bulkload into staging, transforms to final, adds constraints.

set -euo pipefail

#######################################
# Config (override via env if needed) #
#######################################
: "${PGHOST:=127.0.0.1}"
: "${PGPORT:=5432}"
: "${PGDATABASE:=postgres}"
: "${PGUSER:=postgres}"
: "${PGPASSWORD:=postgres}"
export PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD

# CSV filenames (must be in same dir)
AUTHORS_CSV="./authors.csv"
SUBREDDITS_CSV="./subreddits.csv"
SUBMISSIONS_CSV="./submissions.csv"
COMMENTS_CSV="./comments.csv"

for f in "$AUTHORS_CSV" "$SUBREDDITS_CSV" "$SUBMISSIONS_CSV" "$COMMENTS_CSV"; do
  [[ -f "$f" ]] || { echo "Missing required file: $f"; exit 1; }
done

################################
# 1. Create schema & staging   #
################################
cat > schema.sql <<'SQL'
-- schema.sql
DROP TABLE IF EXISTS comments CASCADE;
DROP TABLE IF EXISTS submissions CASCADE;
DROP TABLE IF EXISTS authors CASCADE;
DROP TABLE IF EXISTS subreddits CASCADE;

DROP TABLE IF EXISTS staging_comments;
DROP TABLE IF EXISTS staging_submissions;
DROP TABLE IF EXISTS staging_authors;
DROP TABLE IF EXISTS staging_subreddits;

-- Final tables
CREATE TABLE authors (
  id text PRIMARY KEY,
  retrieved_on bigint,
  name text NOT NULL,
  created_utc bigint,
  link_karma integer,
  comment_karma integer,
  profile_img text,
  profile_color text,
  profile_over_18 boolean
);

CREATE TABLE subreddits (
  id text PRIMARY KEY,
  name text,
  display_name text,
  title text,
  description text,
  public_description text,
  created_utc bigint,
  retrieved_utc bigint,
  subscribers integer,
  header_img text,
  banner_background_image text,
  subreddit_type text,
  whitelist_status text,
  hide_ads boolean,
  over_18 boolean
);

CREATE TABLE submissions (
  id text PRIMARY KEY,
  name text UNIQUE,
  title text,
  url text,
  permalink text,
  author text,
  subreddit_id text NOT NULL,
  created_utc bigint,
  ups integer,
  downs integer,
  num_comments integer,
  edited boolean,
  likes text,
  num_reports integer
);

CREATE TABLE comments (
  id text PRIMARY KEY,
  name text UNIQUE,
  body text,
  author text,
  subreddit_id text,
  subreddit text,
  link_id text NOT NULL,
  parent_id text,
  created_utc bigint,
  retrieved_on bigint,
  ups integer,
  downs integer,
  score integer,
  distinguished text,
  controversiality integer,
  edited boolean,
  gilded integer,
  author_flair_css_class text,
  author_flair_text text,
  score_hidden boolean,
  archived boolean
);

-- Staging (all text)
CREATE TABLE staging_authors (
  id text, retrieved_on text, name text, created_utc text,
  link_karma text, comment_karma text, profile_img text,
  profile_color text, profile_over_18 text
);

CREATE TABLE staging_subreddits (
  banner_background_image text, created_utc text, description text,
  display_name text, header_img text, hide_ads text, id text,
  over_18 text, public_description text, retrieved_utc text,
  name text, subreddit_type text, subscribers text, title text,
  whitelist_status text
);

CREATE TABLE staging_submissions (
  downs text, url text, id text, edited text, num_reports text,
  created_utc text, name text, title text, author text, permalink text,
  num_comments text, likes text, subreddit_id text, ups text
);

CREATE TABLE staging_comments (
  distinguished text, downs text, created_utc text, controversiality text,
  edited text, gilded text, author_flair_css_class text, id text, author text,
  retrieved_on text, score_hidden text, subreddit_id text, score text, name text,
  author_flair_text text, link_id text, archived text, ups text, parent_id text,
  subreddit text, body text
);
SQL

echo "==> Creating schema..."
psql -v ON_ERROR_STOP=1 -f schema.sql

##########################################
# 2. Write pg_bulkload control files     #
##########################################
make_ctl () {
  local ctlfile="$1" csvfile="$2" table="$3"
  cat > "$ctlfile" <<CTL
INPUT = $csvfile
TYPE = CSV
DELIMITER = ','
QUOTE = '"'
ESCAPE = '"'
NULL = ''
HEADER = YES

OUTPUT = $table
LOGFILE = ${ctlfile%.ctl}.log
BADFILE = ${ctlfile%.ctl}.bad
CTL
}

make_ctl bulkload_authors.ctl     "$AUTHORS_CSV"    "staging_authors"
make_ctl bulkload_subreddits.ctl  "$SUBREDDITS_CSV" "staging_subreddits"
make_ctl bulkload_submissions.ctl "$SUBMISSIONS_CSV" "staging_submissions"
make_ctl bulkload_comments.ctl    "$COMMENTS_CSV"   "staging_comments"

##########################################
# 3. Bulk load CSVs                      #
##########################################
echo "==> Bulk loading CSVs with pg_bulkload..."
pg_bulkload bulkload_authors.ctl
pg_bulkload bulkload_subreddits.ctl
pg_bulkload bulkload_submissions.ctl
pg_bulkload bulkload_comments.ctl

##########################################
# 4. Transform staging -> final tables   #
##########################################
cat > transform.sql <<'SQL'
-- transform.sql
\set ON_ERROR_STOP on

INSERT INTO authors
SELECT
  id,
  NULLIF(retrieved_on,'')::bigint,
  name,
  NULLIF(created_utc,'')::bigint,
  NULLIF(link_karma,'')::integer,
  NULLIF(comment_karma,'')::integer,
  profile_img,
  profile_color,
  CASE lower(coalesce(profile_over_18,'')) WHEN 'true' THEN true
       WHEN 'false' THEN false ELSE NULL END
FROM staging_authors WHERE id IS NOT NULL;

INSERT INTO subreddits
SELECT
  id, name, display_name, title, description, public_description,
  NULLIF(created_utc,'')::bigint,
  NULLIF(retrieved_utc,'')::bigint,
  NULLIF(subscribers,'')::integer,
  header_img, banner_background_image,
  subreddit_type, whitelist_status,
  CASE lower(coalesce(hide_ads,'')) WHEN 'true' THEN true WHEN 'false' THEN false ELSE NULL END,
  CASE lower(coalesce(over_18,'')) WHEN 'true' THEN true WHEN 'false' THEN false ELSE NULL END
FROM staging_subreddits WHERE id IS NOT NULL;

INSERT INTO submissions
SELECT
  id, name, title, url, permalink,
  NULLIF(author,''), subreddit_id,
  NULLIF(created_utc,'')::bigint,
  NULLIF(ups,'')::integer,
  NULLIF(downs,'')::integer,
  NULLIF(num_comments,'')::integer,
  CASE lower(coalesce(edited,'')) WHEN 'true' THEN true WHEN 'false' THEN false ELSE NULL END,
  likes,
  NULLIF(num_reports,'')::integer
FROM staging_submissions WHERE id IS NOT NULL;

INSERT INTO comments
SELECT
  id, name, body,
  NULLIF(author,''), subreddit_id, subreddit,
  link_id, parent_id,
  NULLIF(created_utc,'')::bigint,
  NULLIF(retrieved_on,'')::bigint,
  NULLIF(ups,'')::integer,
  NULLIF(downs,'')::integer,
  NULLIF(score,'')::integer,
  distinguished,
  NULLIF(controversiality,'')::integer,
  CASE lower(coalesce(edited,'')) WHEN 'true' THEN true WHEN 'false' THEN false ELSE NULL END,
  NULLIF(gilded,'')::integer,
  author_flair_css_class,
  author_flair_text,
  CASE lower(coalesce(score_hidden,'')) WHEN 'true' THEN true WHEN 'false' THEN false ELSE NULL END,
  CASE lower(coalesce(archived,'')) WHEN 'true' THEN true WHEN 'false' THEN false ELSE NULL END
FROM staging_comments WHERE id IS NOT NULL;
SQL

echo "==> Transforming data..."
psql -v ON_ERROR_STOP=1 -f transform.sql

##########################################
# 5. Constraints & indexes               #
##########################################
cat > constraints.sql <<'SQL'
-- constraints.sql
ALTER TABLE authors ADD CONSTRAINT uq_authors_name UNIQUE (name);
ALTER TABLE submissions ADD CONSTRAINT uq_submissions_name UNIQUE (name);
ALTER TABLE comments ADD CONSTRAINT uq_comments_name UNIQUE (name);
ALTER TABLE subreddits ADD CONSTRAINT uq_subreddits_name UNIQUE (name);

ALTER TABLE submissions
  ADD CONSTRAINT fk_submissions_subreddit FOREIGN KEY (subreddit_id)
  REFERENCES subreddits(id) ON UPDATE CASCADE ON DELETE RESTRICT;

ALTER TABLE submissions
  ADD CONSTRAINT fk_submissions_author FOREIGN KEY (author)
  REFERENCES authors(name) ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_subreddit FOREIGN KEY (subreddit_id)
  REFERENCES subreddits(id) ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_author FOREIGN KEY (author)
  REFERENCES authors(name) ON UPDATE CASCADE ON DELETE SET NULL;

ALTER TABLE comments
  ADD CONSTRAINT fk_comments_link_submission FOREIGN KEY (link_id)
  REFERENCES submissions(name) ON UPDATE CASCADE ON DELETE CASCADE;

CREATE INDEX IF NOT EXISTS idx_submissions_subreddit_id ON submissions(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_submissions_author ON submissions(author);
CREATE INDEX IF NOT EXISTS idx_comments_subreddit_id ON comments(subreddit_id);
CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author);
CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id);
CREATE INDEX IF NOT EXISTS idx_comments_link_id ON comments(link_id);

ANALYZE authors;
ANALYZE subreddits;
ANALYZE submissions;
ANALYZE comments;
SQL

echo "==> Adding constraints & indexes..."
psql -v ON_ERROR_STOP=1 -f constraints.sql

echo "==> Done. Loaded data with pg_bulkload."

