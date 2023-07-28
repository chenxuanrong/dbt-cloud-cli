# Handover

1. how to write `job.yml`, required and optional fields
2. how to write `credential.yml`
3. how does sample option work 
4. save inidivual job run artifact in a row

# Futures


# Get Started

The [collector package repo](https://github.com/chenxuanrong/dbt-cloud-cli)

1. Install
```
pip install git+https://chenxuan.rong:${GITHUB_TOKEN}@github.com/chenxuanrong/dbt-cloud-cli
```

2. Set up environment variables
```
export DBT_CLOUD_ACCOUNT_ID='7108'
export DBT_CLOUD_API_TOKEN='<api_key>'
```

3. Create `credential.yml` and `job.yml`
```
dbt-cloud init
```

## Docs


Motivation

To create a unified data metric platform for engineers and product managers to track health and quality of svp data products.

how the high level structure look like, e.g. operational and business 
metric