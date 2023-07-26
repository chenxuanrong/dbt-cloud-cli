create stage if not exists data_quality_metrics
storage_integration = dqm_integration
url = 's3://data_quality_metrics/'
file_format = (type = json);