from typing import Any
from typing import Dict


__job_registry: Dict[str, Any] = {}

def get_all_automation_jobs():
    return dict(**__job_registry)

def register_job(key:str, job_class: Any):
    __job_registry[key]=job_class