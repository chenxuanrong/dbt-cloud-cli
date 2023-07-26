create table if not exists zz_chenxuan_rong_dev.metrics.src_operation_metric (
  project varchar,
  run_id integer,
  data variant,
  generated_at datetime,
  inserted_at datetime
);
