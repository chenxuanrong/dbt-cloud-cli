from dbt_cloud.exc import DbtCloudConfigError, DbtCloudException
from dataclasses import dataclass
from typing import List

from dbt_cloud import safe_load_yaml
import os

DBT_CLOUD_CONFIG_PATH = os.path.join(os.getcwd(), 'job.yml')

@dataclass
class Environment(object):
    name: str
    id: str
    account_id: str
    project_id: str
    dbt_version: str
    custom_branch: str

@dataclass
class Job(object):
    name: str
    execute_steps: list
    tracking: bool
    schedule: str
    environment_id: str
    job_id: str
    project_id: str
    dbt_version: str
    generate_docs: bool
    run_generate_sources: bool


class Configuration(object):
    def __init__(self, account_id: str, project_name: str, environments: Environment, jobs: List[Job]) -> None:
        self.account_id = account_id
        self.project_name = project_name
        self.environments = environments
        self.jobs = jobs
    
    @classmethod
    def load(cls, config_path=DBT_CLOUD_CONFIG_PATH):
        config = safe_load_yaml(config_path)
        if config is None:
            raise DbtCloudConfigError("configuration file error")
        
        account_id = config.get("account_id") 
        project_name = config.get('project_name')
        project_id = config.get("project_id")

        environments = []
        jobs = []
        for _environment in config.get("environments", []):
            environment_name = _environment.get("name")
            environment_id = _environment.get("id")
            e = Environment(
                name=environment_name, 
                id=environment_id,
                dbt_version=_environment.get("dbt_version"), 
                custom_branch=_environment.get("custom_branch"), 
                account_id=account_id, 
                project_id=project_id
            )
            environments.append(e)

            _jobs = _environment.get("jobs", [])
            for job in _jobs:
                j = Job(
                    project_id=project_id,
                    job_id=job.get("id"),
                    environment_id=environment_id,
                    name=job.get("name"),
                    execute_steps=job.get("steps", []),
                    dbt_version=job.get("dbt_version"),
                    tracking=job.get("tracking", False),
                    schedule=job.get("schedule"),
                    generate_docs=job.get("generate_docs", False),
                    run_generate_sources=job.get("run_generate_sources", False),
                )
                jobs.append(j)
        
        return cls(
            account_id=account_id,
            project_name=project_name,
            environments=environments,
            jobs=jobs
        )
    


    