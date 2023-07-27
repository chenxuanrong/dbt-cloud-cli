
import os
import json
from datetime import datetime 
from flask import Flask, jsonify, request
from snowflake.sqlalchemy import (VARIANT, ARRAY, OBJECT)


import requests

import snowflake

from db import SnowflakeConnector
from utils import datetime_to_str
from security import api_required, generate_auth_token
from status import HTTP_200_OK, HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED, HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR

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
connection = engine.connect()

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
# @api_required
def get_operational_metric():
    data = []
    results = connection.execute(f"select * from {tgt_db}.{tgt_schema}.src_operation_metric").fetchall()
    for c in results:
        data.append({
            "project": c[0],
            "run_id": c[1],
            "metric": json.loads(c[2]),
            "generated_at": c[3],
            "inserted_at": c[4],
        })
    return jsonify(data), HTTP_200_OK

@app.route('/metric/operational/<node_id>', methods=['GET'])
def get_operational_metric_by_id(node_id):
    data = []
    try:
        results = connection.execute(f"""
            select distinct
            data:unique_id::string as unique_id,
            data:affected_rows::int as affected_rows,
            round(data:execution_time,2)::numeric(10,1) as execution_time,
            data:status::string as status,
            data:collected_at::timestamp_tz as collected_at
            from {tgt_db}.{tgt_schema}.src_operation_metric
            where data:unique_id = '{node_id}'
            order by collected_at""")
        for c in results:
            data.append({
                "unique_id": c[0],
                "affected_row": c[1],
                "execution_time": c[2],
                "status": c[3],
                "collected_at": c[4]            
            })
        return jsonify(data), HTTP_200_OK
    except Exception as e:
        print(e)
        return HTTP_500_INTERNAL_SERVER_ERROR


# Example endpoint to get business metrics
@app.route('/metric/business', methods=['GET'])
def get_business_metric():
    data = []

    results = connection.execute(f"select * from {tgt_db}.{tgt_schema}.src_business_metric").fetchall()
    for c in results:
        data.append(c)
    return jsonify(data), HTTP_200_OK


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
        return jsonify({"message": f"Operational metric added successfully, {results[0]} inserted"}), HTTP_200_OK
    except snowflake.connector.errors.ProgrammingError as e:
        print(e)
        print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        return jsonify({"error": str(e)}), HTTP_400_BAD_REQUEST

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

        return jsonify({"message": "Business metric added successfully"}), HTTP_200_OK
    except Exception as e:
        return jsonify({"error": str(e)}), HTTP_400_BAD_REQUEST

@app.route('/metric/version', methods=['GET'])
def get_version():
    result = connection.execute(
        "select current_version()"
    ).fetchone()
    return jsonify({"version": result[0]})

@app.route('/metric/ping', methods=['GET'])
def ping():
    return jsonify({"message": "pong"})

@app.route('/api/token')
def get_auth_token():
    token = generate_auth_token()
    # return jsonify({ 'token': token.decode('ascii') })
    return jsonify({'token': token})

if __name__ == '__main__':
    app.run(debug=True)