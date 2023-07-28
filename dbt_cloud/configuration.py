import os
from typing import List
from dataclasses import dataclass

from rich.console import Console

from dbt_cloud import safe_load_yaml
from dbt_cloud.exc import DbtCloudConfigError, DbtCloudException
from dbt_cloud.datasource import SnowflakeCredentialError


DBT_CLOUD_CONFIG_PATH = os.path.join(os.getcwd(), 'job.yml')
DS_CONFIG_PATH = os.path.join(os.getcwd(), 'credential.yml')

CONSOLE_MSG_PASS = '[bold green]✅ PASS[/bold green]\n'
CONSOLE_MSG_FAIL = '[bold red]😱 FAILED[/bold red]\n'
CONSOLE_MSG_ALL_SET = '[bold]🎉 You are all set![/bold]\n'


console = Console()

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
    def __init__(self, account_id: str, project_name: str, environments: Environment, jobs: List[Job], project_id: int) -> None:
        self.account_id = account_id
        self.project_id = project_id
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
            project_id=project_id,
            environments=environments,
            jobs=jobs
        )
    

    @staticmethod
    def load_credentials(config_path=DS_CONFIG_PATH):
        config = safe_load_yaml(config_path)
        if config is None:
            raise DbtCloudConfigError("configuration file error")
        
        for v in config.values():
            from dbt_cloud.datasource import SnowflakeConnector
            SnowflakeConnector.validate(v)
            src = {
                'account': v.get('account'),
                'user': v.get('user'),
                'database': v.get('database'),
                'role': v.get('role'),
                'warehouse': v.get('warehouse'),
                'schema': v.get('schema'),
                'authenticator': v.get('authenticator'),
                'stage': v.get('stage'),
                'password': v.get('password'),
            }
            return src


    def validate(self):
        all_passed = True
        failed_reasons = []

        console.print('Check config files:')
        console.print( f'{DBT_CLOUD_CONFIG_PATH}: [[bold green]OK[/bold green]]')
        console.print( f'{DS_CONFIG_PATH}: [[bold green]OK[/bold green]]')
        console.print()
        
        console.print('Check account:')
        if not self.account_id:
            console.print('account: [[bold red]FAILED[/bold red]] reason: account id is missing')
            failed_reasons.append("Account ID is missing")
        else:
            console.print('account: [[bold green]OK[/bold green]]')
        console.print()

        console.print('Check project:')
        if not self.project_id:
            console.print('project: [[bold red]FAILED[/bold red]] reason: project id is missing')
            failed_reasons.append("Project ID is missing")
        else:
            console.print('project: [[bold green]OK[/bold green]]')
        console.print()

        console.print('Check jobs:')
        for job in self.jobs:
            if not isinstance(job.job_id, int):
                console.print(f'    [[bold red]FAILED[/bold red]] {job.name} job id {job.job_id} is not recognisable')
                failed_reasons.append(f"Job ID {job.job_id} is not recognisable. Expecting an integer.")
            else:
                console.print(f'    [[bold green]OK[/bold green]] {job.name}')
        console.print()
    
        if len(failed_reasons) == 0:
            console.print(CONSOLE_MSG_PASS)
            console.print(CONSOLE_MSG_ALL_SET)
        else:
            console.print(CONSOLE_MSG_FAIL)