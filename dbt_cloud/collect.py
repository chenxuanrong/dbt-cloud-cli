import json
from typing import List, Any
from dbt_cloud.configuration import Configuration, Job, Environment
from dbt_cloud.command.job.list import DbtCloudJobListCommand
from dbt_cloud.command.run.list import DbtCloudRunListCommand
from dbt_cloud.command.run.get_artifact import DbtCloudRunGetArtifactCommand
from dbt_cloud.serde import json_to_dict
from rich.console import Console
import requests
from dbt_cloud import write_to_file

console = Console()

class Collector(object):
    def __init__(self, configurator: Configuration, limit: int = 100) -> None:
        self.configurator=configurator
        self.limit = limit

    
    def collect(self) -> List[Any]:
        tracking_jobs = [job for job in self.configurator.jobs if job.tracking]
        results = []
        console.print(f"Detected {len(tracking_jobs)} jobs")

        for job in tracking_jobs:                      
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
                    # results.extend(metrics)
                    results.extend(metrics)
        write_to_file(results)

                      
    def upload(self) -> requests.Response:
        response = requests.post(
            url=self.api_url, headers=self.request_headers, json=self.get_payload()
        )
        return response
  
if __name__ == '__main__':
    configuration = Configuration.load()

    collector = Collector(configurator=configuration)
    results = collector.collect()
    console.print(results)
        