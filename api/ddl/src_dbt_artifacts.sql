create table if not exists zz_chenxuan_rong_dev.metrics.src_dbt_artifacts (
  data variant,
  generated_at timestamp_ltz,
  path varchar,
  artifact_type varchar
);