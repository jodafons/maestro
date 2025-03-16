
__all__ = []

from typing import Dict, Any, Union, List
from pydantic import BaseModel




class TaskInputs(BaseModel):
    name           : str=""
    input          : str=""
    command        : str=""
    image          : str=""
    outputs        : Dict[str,str]={}
    secondary_data : Dict[str,str]={}
    envs           : Dict[str,str]={}
    device         : str="cpu"
    memory_mb      : int=-1
    gpu_memory_mb  : int=-1
    cpu_cores      : int=-1
    
    def get_output_data(self, key:str) -> str:
        return f"{self.name}.{self.outputs[key]}"
    
class TaskInfo(BaseModel):
    name           : str=""
    task_id        : str=""
    user_id        : str=""
    partition      : str=""
    status         : str=""
    flavor         : str=""
    counts         : Dict[str,int]={}
    jobs           : List[str]=[]
    
class Job(BaseModel):
    job_id          : str=""
    envs            : Dict={}
    command         : str=""

