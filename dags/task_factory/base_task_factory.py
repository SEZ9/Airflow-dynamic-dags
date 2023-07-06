from abc import ABC, ABCMeta, abstractmethod
from typing import List

from airflow.utils.task_group import TaskGroup

from config.conf import Config

class BaseTaskFactory(metaclass=ABCMeta):
    
    @abstractmethod
    def get_task_group_instance(self) -> TaskGroup:
        pass