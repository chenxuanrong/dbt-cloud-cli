
import os
import json
from datetime import datetime 
from flask import Flask, jsonify, request
from snowflake.sqlalchemy import (VARIANT, ARRAY, OBJECT)

import requests

import snowflake

from db import SnowflakeConnector
from utils import datetime_to_str

snowflake_account = 're61407.ap-southeast-2'
snowflake_user = 'chenxuan.rong'
snowflake_database = 'zz_chenxuan_rong_dev'
snowflake_schema = 'sandbox'
snowflake_warehouse = 'warehouse_xs'
snowflake_auth = 'externalbrowser'
snowflake_role="data_dept_property_dev"

db = SnowflakeConnector(
    account=snowflake_account,
    user=snowflake_user,
    database=snowflake_database,
    schema=snowflake_schema,
    warehouse=snowflake_warehouse,
    auth=snowflake_auth
)

engine = db.create_engine()

tgt = os.getenv("target", "staging")

env = {
    "dev": {
        "db": "zz_chenxuan_rong_dev",
        "schema": "sandbox",
    },
    "staging": {
        "db": "zz_chenxuan_rong_dev",
        "schema": "metrics",
    },
    "production": {
        "db": "property_data_staging",
        "schema": "metrics",
    }
}

tgt_db = env.get(tgt).get("db")
tgt_schema = env.get(tgt).get("schema")

app = Flask(__name__)


# Example data for demonstration purposes
operational_metrics_data = {
    "metric": "Operational Metric",
    "value": 100
}

business_metrics_data = {
    "metric": "Business Metric",
    "value": 500
}

# Example endpoint to get operational metrics
@app.route('/metric/operational', methods=['GET'])
def get_operational_metric():
    data = []
    with engine.connect() as connection:
        results = connection.execute(f"select * from {tgt_db}.{tgt_schema}.src_operation_metric").fetchall()
        for c in results:
            data.append({
                "project": c[0],
                "run_id": c[1],
                "metric": json.loads(c[2]),
                "generated_at": c[3],
                "inserted_at": c[4],
            })
        return jsonify(data)

# Example endpoint to get business metrics
@app.route('/metric/business', methods=['GET'])
def get_business_metric():
    data = []
    with engine.connect() as connection:
        results = connection.execute(f"select * from {tgt_db}.{tgt_schema}.src_business_metric").fetchall()
        for c in results:
            data.append(c)
        return jsonify(data)    


@app.route('/metric/operational', methods=['POST'])
def add_operational_metric():
    data = request.get_json()
    project = data.get("project")
    run_id = data.get("run_id")
    metrics = data.get("metrics", [])
    generated_at = data.get("generated_at")
    inserted_at = datetime_to_str(datetime.now())

    # ser_metrics = json.dumps(metrics)    

    print(f"""
    project: {project}
    run_id: {run_id}
    metric: {metrics}
    generated_at: {generated_at}      
""")
    
    try:
        connection = engine.connect()
        stmt = []
        for m in metrics:
            st = f"select '{project}', '{run_id}', parse_json('{json.dumps(m)}'), '{generated_at}', '{inserted_at}'"
            stmt.append(st)
    
        insmt="\nunion all\n".join(stmt)
        print(stmt)
        results= (10,0)

        results = connection.execute(
                f"insert into {tgt_db}.{tgt_schema}.src_operation_metric(project, run_id, data, generated_at, inserted_at)"
                f"{insmt}"
               ).fetchone()
        print(results[0])
        return jsonify({"message": f"Operational metric added successfully, {results[0]} inserted"})
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return jsonify({"error": str(e)}), 400
    finally:
        connection.close()

# Example endpoint to add new business metric
@app.route('/metric/business', methods=['POST'])
def add_business_metric():
    try:
        data = requests.get_json()
        # Assuming the POST data should have "metric" and "value" keys
        metric = data['metric']
        value = data['value']

        # Here, you would insert the business metric data into Snowflake
        # For demonstration purposes, we will just update the static data
        business_metrics_data["metric"] = metric
        business_metrics_data["value"] = value

        return jsonify({"message": "Business metric added successfully"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/metric/version', methods=['GET'])
def get_version():
    with engine.connect() as connection:
        result = connection.execute(
            "select current_version()"
        ).fetchone()
        return jsonify({"version": result[0]})

if __name__ == '__main__':
    app.run(debug=True)