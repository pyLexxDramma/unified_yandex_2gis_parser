import uuid
from typing import Dict, Any, List, Optional


class TaskStatus:
    def __init__(self, task_id: str, status: str, progress: str, email: Optional[str] = None,
                 source_info: Optional[Dict[str, Any]] = None):
        self.task_id: str = task_id
        self.status: str = status
        self.progress: str = progress
        self.email: Optional[str] = email
        self.source_info: Optional[Dict[str, Any]] = source_info
        self.detailed_results: List[Dict[str, Any]] = []
        self.statistics: Dict[str, Any] = {}
        self.result_file: Optional[str] = None
        self.error: Optional[str] = None
        self.timestamp = uuid.uuid4()

    def __repr__(self):
        return (f"TaskStatus(task_id='{self.task_id}', status='{self.status}', "
                f"progress='{self.progress}', email='{self.email}', "
                f"source_info={self.source_info})")


active_tasks: Dict[str, TaskStatus] = {}
