import json
import os
from typing import List, Any
from dbt_cloud.configuration import Configuration, Job, Environment
from dbt_cloud.command.job.list import DbtCloudJobListCommand
from dbt_cloud.command.run.list import DbtCloudRunListCommand
from dbt_cloud.command.run.get_artifact import DbtCloudRunGetArtifactCommand
from dbt_cloud.serde import json_to_dict
from rich.console import Console
import requests
from dbt_cloud import write_to_file
from dbt_cloud.datasource import SnowflakeConnector

console = Console()
URL = "http://127.0.0.1:5000/metric/operational"
PING= "http://127.0.0.1:5000/metric/ping"

class Collector(object):
    def __init__(self, configurator: Configuration, limit: int = 5, datasource=None) -> None:
        self.configurator=configurator
        self.limit = limit
        self.api_url = URL
        self.datasource = datasource
    
    def collect(self, debug=False, upload=False, job_id=None, archive=True) -> List[Any]:
        tracking_jobs = [job for job in self.configurator.jobs if job.tracking]
        payloads = []
        raw_artifacts = []
        if debug:
            console.print(tracking_jobs)
        if job_id:
            selected_jobs = [job for job in tracking_jobs if job.job_id == job_id]
            console.print(job_id)
            console.print(f"Finding job {len(selected_jobs)}")
        else:
            selected_jobs = tracking_jobs[0]
            console.print(f"Collecting {len(tracking_jobs)}")
        # console.print(f"Default to collection first job: {tracking_jobs[0].name}")

        if len(selected_jobs) == 0:
            console.print("No job selected")
        else:
            for job in selected_jobs:
                job_id = job.job_id
                job_name = job.name
                project_id = job.project_id

                res = DbtCloudRunListCommand(
                    project_id=project_id,
                    job_id=job_id,
                    limit=self.limit,
                    order_by='-created_at'
                ).execute().json()
            
                jobruns = res.get("data", [])
                console.print(f"Fetched {len(jobruns)} runs for job id {job_id}")

                for idx, run in enumerate(jobruns):

                    run_id = run.get("id")
                    res=DbtCloudRunGetArtifactCommand(
                        run_id=run_id,
                        path="run_results.json"
                    ).execute()

                    if res.status_code == 200:
                        data = res.json()
                        raw_artifacts.append(data)
                        dbt_version = data.get("metadata").get("dbt_version")
                        collected_at = data.get("metadata").get("generated_at")
                        nodes = data.get("results", [])

                        metrics = [
                            {
                                "unique_id": node.get("unique_id"), 
                                "job_id": job_id,
                                "job_name": job_name,
                                "run_id": run_id,
                                "dbt_version": dbt_version,
                                "execution_time": node.get("execution_time"),
                                "affected_rows": node.get("adapter_response").get("rows_affected"),
                                "status": node.get("status"),
                                "collected_at": collected_at
                            } 
                            for node in nodes
                        ]
                        console.print(f"{idx+1}/{len(jobruns)} found {len(metrics)} nodes")
                        payloads.append({
                            "project": "svp",
                            "run_id": run_id,
                            "metrics": metrics,
                            "generated_at": collected_at
                        })
                    else:
                        console.print(f"{idx+1}/{len(jobruns)} artifacts not found. Run Id {run_id}")
                        
            if debug:
                filepath = write_to_file(data=payloads, name='metric.json')
                rawfilepath = write_to_file(data=raw_artifacts, name='run_results.json')
                self.archive([rawfilepath])
            if upload:
                console.print("Uploading report...")
                self.upload(payloads)
                      
    def upload(self, json:List) -> requests.Response:

        request_headers = {
            "Content": "application/json"            
        }

        if not isinstance(json, List):
            raise Exception("Payload should be a list")
        
        try:
            ping = requests.post(
                url=PING,
                headers=request_headers
            )

            if not ping.status_code != 200:
                console.print('Cloud API is down')
            else:
                for j in json:
                    response = requests.post(
                        url=self.api_url, 
                        headers=request_headers, 
                        json=j
                    )
                    if response.status_code == 200:
                        console.print(f"Results uploaded to {self.api_url}")
                return response
        except requests.exceptions.ConnectionError:
            console.print('Cloud API endpoint is not connected')
    
    def archive(self, filepath=[]) -> None:
        db = SnowflakeConnector(self.datasource)
        try:
            engine = db.create_engine()
            with engine.connect() as connection:
                for fp in filepath:
                    clmt = f"remove @{db.stage} pattern='.*.json.gz';"
                    ptmt = f"put file://{fp} @{db.stage} auto_compress=true;"
                    cpmt = f"""
                    copy into {db.database}.{db.schema}.src_dbt_artifacts from
                        (
                            select
                            $1 as data,
                            $1:metadata:generated_at::timestamp_ntz as generated_at,
                            metadata$filename as path,
                            regexp_substr(metadata$filename, '([a-z_]+.json)') as artifact_type
                            from  @{db.stage}
                        )
                    file_format=(type='JSON')
                    on_error='skip_file';
                    """
                    connection.execute(clmt)
                    connection.execute(ptmt)
                    connection.execute(cpmt)
                    console.print()
        except FileNotFoundError as e:
            console.print(e)
  
if __name__ == '__main__':
    configuration = Configuration.load()

    collector = Collector(configurator=configuration)
    results = collector.collect()
    console.print(results)
        