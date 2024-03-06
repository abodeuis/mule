import logging
from enum import Enum

class p_worker_status(Enum):
    WORKER_STARTING = 1
    WORKER_STOPPING = 2
    STARTED_PROCESSING = 3
    COMPLETED_PROCESSING = 4
    ERROR = 5
    LOG_MESSAGE = 6
    USER_MESSAGE = 7

class p_worker_status_message():
    def __init__(self, process_id:int, step_id:int, item_id:int, status:p_worker_status, item_name=None, log_level:int=logging.DEBUG, message:str=None):
        self.pid = process_id
        self.step_id = step_id
        self.item_id = item_id
        self.item_name = item_name
        self.status = status
        self.log_level = log_level
        self.message = message

    def __str__(self):
        outstr = 'p_worker_status_message{'
        outstr += f'pid : {self.pid}, sid : {self.step_id}, iid : {self.item_id}, status : {self.status}, log_level : {self.log_level}, message : {self.message}'
        outstr += '}'    
        return outstr

class p_data_message():
    def __init__(self, id:int, data, name:int=None):
        self.id = id
        self.name = name# Human friendly name for data
        self.data = data