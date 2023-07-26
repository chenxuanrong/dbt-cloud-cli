create table if not exists zz_chenxuan_rong_dev.metrics.src_business_metric (
  project varchar,
  run_id integer,
  version varchar,
  unique_id varchar,
  name varchar,
  description varchar,
  data variant,
  metric_type varchar,
  created_at datetime,
  inserted_at datetime
);