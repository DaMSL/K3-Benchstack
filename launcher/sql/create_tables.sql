CREATE TABLE IF NOT EXISTS trials (
  system        text,
  query         text,
  dataset       text,
  trial         integer,
  elapsed_ms    double precision,
  ts            timestamp 
);
