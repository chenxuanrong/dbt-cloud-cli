import os
import shutil
from rich.console import Console
from dbt_cloud import data
from dbt_cloud.configuration import DBT_CLOUD_CONFIG_PATH, DS_CONFIG_PATH

console = Console()

def _is_job_configuration_exists(workspace_path: str) -> bool:
    if not os.path.exists(os.path.join(workspace_path, 'job.yml')):
        return False
    if not os.path.exists(os.path.join(workspace_path, 'credential.yml')):
        return False
    return True

def clone_file(src, dst):
    shutil.copyfile(src, dst)
    

class Initializer(object):
    def __init__(self) -> None:
        pass
    
    @staticmethod
    def exec():
        working_dir = os.getcwd()
        if _is_job_configuration_exists(working_dir):
            console.print('[bold green]Configuration already exists.[/bold green]')
            console.print(DBT_CLOUD_CONFIG_PATH)
            console.print(DS_CONFIG_PATH)
        else:
          from dbt_cloud import data
          init_template_dir = os.path.dirname(data.__file__)
          workspace_dir = os.getcwd()
          
          clone_file(os.path.join(init_template_dir, 'credential.yml'), os.path.join(workspace_dir, 'credential.yml'))
          clone_file(os.path.join(init_template_dir, 'job.yml'), os.path.join(workspace_dir, 'job.yml'))
          console.print(f'[bold green]Initialised project configuration files: credential.yml, job.yml[/bold green]')
          

