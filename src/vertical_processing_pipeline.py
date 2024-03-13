import logging
import traceback
import multiprocessing
import pandas as pd
from rich import box
from rich.live import Live
from rich.table import Table
from typing import List
from time import time, sleep

from src.utils import RichHandler, swap_console_handler
from src.pipeline_communication import p_data_message, p_worker_status_message, p_worker_status

log = logging.getLogger('CMASS_MULE')

class parameter_data_stream():
    def __init__(self, data:List[any]=[], names:List[str]=None, max_size:int=None):
        mpm = multiprocessing.Manager()
        if max_size is None:
            self._queue = mpm.Queue()
        else:
            self._queue = mpm.Queue(maxsize=max_size)
        self._id_counter = 0

        for item in data:
            name = names[self._id_counter] if names is not None else None
            dm = p_data_message(self._id_counter, item, name=name)
            self._queue.put(dm)
            self._id_counter += 1

    def append(self, data:any, id:int=None, name:str=None):
        if id is None:
            id = self._id_counter
            self._id_counter += 1
        dm = p_data_message(id, data, name=name)
        self._queue.put(dm)

    def get(self):
        return self._queue.get()

    def __len__(self):
        return self._queue.qsize()
    
    def empty(self):
        return self._queue.empty()

    def full(self):
        return self._queue.full()
    
class internal_parameter_data(parameter_data_stream):
    """Interal class to pass parameters bewteen pipeline steps"""
    def __init__(self, id:int=None, data:any=None, name:str=None):
        self.id = id
        self.data = data
        self.name = name

    def append(self, id:int, data:any, name:str=None):
        self.id = id
        self.data = data
        self.name = name
    
    def get(self):
        return p_data_message(self.id, self.data, name=self.name)
    
    def __len__(self):
        return int(self.id == None)

    def empty(self):
        return self.id is None
    
    def full(self):
        return self.id != None

class vertical_pipeline_step():
    def __init__(self, id, func, args, name=''):
        self.id = id
        self.func = func
        self.args = args
        self.name = name # Used for status messages
        self._output = internal_parameter_data()

    def output(self):
        """Returns a stream to the output of this step."""
        return self._output

class console_monitor():
    def __init__(self, title='Pipeline Monitor', timeout=1, refesh=0.25, max_lines=20):
        self.title = title
        self.timeout = timeout 
        self.refesh = refesh
        self.max_lines = max_lines
        
        self._final_step = None
        self._data_df = pd.DataFrame(columns=['item_id', 'step_id', 'processing_time', 'last_update', 'name'])
        self._step_name_lookup = {None : 'Waiting in queue', 'FINISHED' : 'Done processing', 'ERROR': 'ERROR'}

    def active(self):
        for value in self._data_df['step_id'].unique():
            if value not in ['FINISHED', 'ERROR']:
                return True
        return False

    def add_step(self, id, name):
        self._step_name_lookup[str(id)] = name
        self._final_step = id

    def update_data(self, record:p_worker_status_message) -> pd.DataFrame:
        df = self._data_df
        # Get row of map
        if record.item_id is None and type(record.message) == dict: # Find item_id for user supplied messages
            record.item_id = df[df['name'] == record.message['name']]['item_id'].values[0]
        
        if record.item_id in df['item_id'].values:
            irow = df[df['item_id'] == record.item_id].index[0]
        else:
            irow = None

        # Don't updated finished items
        if irow is not None and df.at[irow, 'step_id'] == 'FINISHED':
            return

        if record.status == p_worker_status.STARTED_PROCESSING:
            if irow is not None:
                df.at[irow, 'step_id'] = str(record.step_id)
                df.at[irow, 'last_update'] = time()
            if record.item_id not in df['item_id'].values:
                df.loc[len(df)] = {'item_id': record.item_id, 'step_id': str(record.step_id), 'processing_time': 0.0, 'last_update': time(), 'name': record.item_name, 'shape' : None, 'map_units' : None}
        
        elif record.status == p_worker_status.COMPLETED_PROCESSING:
            df.at[irow, 'step_id'] = None
            if record.step_id == self._final_step: # Last step
                df.at[irow, 'step_id'] = 'FINISHED'

        elif record.status == p_worker_status.ERROR:
            df.at[irow, 'step_id'] = 'ERROR'
        
        elif record.status == p_worker_status.USER_MESSAGE:
            assert type(record.message) == dict, 'User message must be a dictionary'
            assert 'name' in record.message.keys(), 'User message must have a name key'
            for key, value in record.message.items():
                if key not in df.columns: # If column does not exist, add it
                    df.insert(2, key, [None for _ in range(len(df))])
                irow = df[df['name'] == record.message['name']].index[0]
                df.at[irow, key] = value

        self._data_df = df

    def generate_table(self) -> Table:
        # Bulid table structure
        table = Table(title=self.title, expand=True, box=box.MINIMAL)
        table.add_column('Name') # Name first.
        for col in self._data_df.columns: # User supplied columns next
            if col not in ['item_id', 'step_id', 'processing_time', 'last_update', 'name']:
                table.add_column(col)
        table.add_column('Status')
        table.add_column('Processing Time')

        # Update active item's processing time
        for index, row in self._data_df.iterrows():
            if row['step_id'] not in ['FINISHED','ERROR', None]:
                now = time()
                self._data_df.at[index, 'processing_time'] += now - row['last_update']
                self._data_df.at[index, 'last_update'] = now

        # Populate table data
        total_lines = 0
        for index, row in self._data_df.iterrows():
            if total_lines >= self.max_lines:
                break
            if row['step_id'] in ['FINISHED','ERROR']: # Skip completed items
                continue
            color = ''
            if row['step_id'] is not None:
                color = '[green]'
            name = row['name'] if row['name'] is not None else row['item_id']
        
            item_cols = []
            item_cols.append(f'{color}{name}')
            for col in self._data_df.columns:
                if col not in ['item_id', 'step_id', 'processing_time', 'last_update', 'name']:
                    item_cols.append(f'{color}{row[col]}')
            item_cols.append(f'{color}{self._step_name_lookup[row["step_id"]]}')
            item_cols.append(f'{color}{row["processing_time"]:.2f} seconds')
            table.add_row(*item_cols)
            total_lines += 1

        # Add completed items at the bottom
        for index, row in self._data_df[::-1].iterrows():
            if total_lines >= self.max_lines:
                break
            if row['step_id'] not in ['FINISHED','ERROR']: # Skip in progress items
                continue
            color = '[bright_black]'
            if row['step_id'] == 'ERROR':
                color = '[red]'
            name = row['name'] if row['name'] is not None else row['item_id']

            item_cols = []
            item_cols.append(f'{color}{name}')
            for col in self._data_df.columns:
                if col not in ['item_id', 'step_id', 'processing_time', 'last_update', 'name']:
                    item_cols.append(f'{color}{row[col]}')
            item_cols.append(f'{color}{self._step_name_lookup[row["step_id"]]}')
            item_cols.append(f'{color}{row["processing_time"]:.2f} seconds')
            table.add_row(*item_cols)
            total_lines += 1

        return table

def _start_worker(pipeline_steps, log_stream, management_stream):
    def work_ready(args):
        for arg in args:
            if isinstance(arg, parameter_data_stream):
                if arg.empty():
                    return False
        return True

    # Expose the pipeline log stream to the new process
    global pipeline_log_stream
    pipeline_log_stream = log_stream

    pid = multiprocessing.current_process().pid
    msg = p_worker_status_message(pid, None, None, p_worker_status.WORKER_STARTING, log_level=logging.DEBUG, message=f'Worker Process {pid} - Starting')
    log_stream.put(msg)
    while True:
        try:
            # Check for stop message
            if not management_stream.empty():
                message = management_stream.get()
                if message == 'STOP':
                    msg = p_worker_status_message(pid, None, None, p_worker_status.WORKER_STOPPING, log_level=logging.DEBUG, message=f'Worker Process {pid} - Exiting')
                    log_stream.put(msg)
                    break

            # Wait for work
            if not work_ready(pipeline_steps[0].args):
                sleep(0.1)
                continue

            for step in pipeline_steps:
                func_args = []
                arg_data = None
                for arg in step.args:
                    if isinstance(arg, parameter_data_stream):
                        arg_data = arg.get()
                        func_args.append(arg_data.data)
                    else:
                        func_args.append(arg)

                # Run function
                msg = p_worker_status_message(pid, step.id, arg_data.id, p_worker_status.STARTED_PROCESSING, item_name=arg_data.name, log_level=None, message=f'Process {pid} - Started {step.name} : {arg_data.name}')
                log_stream.put(msg)
                result = step.func(*func_args)
                msg = p_worker_status_message(pid, step.id, arg_data.id, p_worker_status.COMPLETED_PROCESSING, item_name=arg_data.name, log_level=None, message=f'Process {pid} - Completed {step.name} : {arg_data.name}')
                log_stream.put(msg) 

                step._output.id = arg_data.id
                step._output.data=result
                step._output.name=arg_data.name
            
            # Clear step outputs between maps
            for step in pipeline_steps:
                step._output.id = None
                step._output.data = None
                step._output.name = None
        
        except Exception as e:
            # Log errors
            if arg_data is not None and arg_data.id:
                msg = p_worker_status_message(pid, step.id, arg_data.id, p_worker_status.ERROR, log_level=logging.ERROR, message=f'Process {pid} - Error in step {step.name} on {arg_data.name} : {e}\n{traceback.format_exc()}')
            else:
                msg = p_worker_status_message(pid, step.id, None, p_worker_status.ERROR, log_level=logging.ERROR, message=f'Process {pid} - Error in step {step.name} : {e}\n{traceback.format_exc()}')
            log_stream.put(msg)



class vertical_processing_pipeline():
    def __new__(cls): # Singleton Pattern
        if not hasattr(cls, 'instance'):
            cls.instance = super(vertical_processing_pipeline, cls).__new__(cls)
        return cls.instance
    
    def __init__(self, workers=multiprocessing.cpu_count()):
        self.steps = []
        self.step_dict = {}
        self.workers = workers
        self._worker_handles = []
        self._running = False
        self._monitor = console_monitor()

        mpm = multiprocessing.Manager()
        self._log_stream = mpm.Queue()
        self._management_stream = mpm.Queue()

    def __getitem__(self, key):
        return self.steps[self.step_dict[key]]

    def next_step_id(self):
        return len(self.steps) - 1

    def add_step(self, func, args, name=''):
        if self._running:
            log.error('Cannot add step while running pipeline. Please stop pipeline before adding steps')
            return
        sid = self.next_step_id()
        step = vertical_pipeline_step(sid, func, args, name)
        self.steps.append(step)
        self.step_dict[name] = sid
        self._monitor.add_step(sid, name)

    def running(self):
        """Returns True if the pipeline is running."""
        return self._running 

    def _create_worker(self):
        w = multiprocessing.Process(target=_start_worker, args=(self.steps, self._log_stream, self._management_stream))
        w.start()
        self._worker_handles.append(w)
    
    def start(self):
        if self._running:
            log.warning('Start was called when Inference pipeline already running. Ignoring call to start')
            return False
        
        for i in range(self.workers):
            self._create_worker()
        self._running = True
        log.info(f'Starting pipeline manager with {len(self.steps)} steps and {len(self._worker_handles)} workers')
        return True
    
    def stop(self):
        if not self._running:
            log.warning('Stop was called when Inference pipeline already stopped. Ignoring call to stop')
            return False

        log.info('Stopping pipeline')
        for w in self._worker_handles:
            self._management_stream.put('STOP')

        # Wait for workers to stop
        log.debug('Waiting for pipeline workers to finish')
        [w.join() for w in self._worker_handles]
        self._running = False
        return True

    def monitor(self):
        _monitor = self._monitor
        with Live(_monitor.generate_table(), refresh_per_second=(1/_monitor.refesh)) as live:
            logging_handler = swap_console_handler(log, RichHandler(live))
            last_activity = time()
            while self._running:
                # Check if there is any in progress maps
                if _monitor.active():
                    last_activity = time()
                if time() - last_activity > _monitor.timeout:
                    break

                # Sleep while no new messages are available
                if self._log_stream.empty():
                    live.update(_monitor.generate_table())
                    sleep(_monitor.refesh)
                    continue

                # Retieve worker messages
                record = self._log_stream.get()
                if record.log_level is not None and record.message is not None:
                    log.log(record.log_level, record.message)
                
                # Update monitor table
                if record.step_id is not None:
                    _monitor.update_data(record)
                live.update(_monitor.generate_table())
            swap_console_handler(log, logging_handler)

        # Stop pipeline when done
        log.info(f'Pipeline Manager has detected that there are no more maps to process. No updates in the last {_monitor.timeout} seconds')
        self.stop()

    def log_to_monitor(item_name, dict):
        # Put message into pipeline log stream
        # This global variable gets set when the worker is started.
        # If you find a better way to do this please fix But this is the only way i could figure out to make the api look nice.
        # so that a user just has to call horizontal_pipeline_manager.log_to_monitor in their own functions
        dict['name'] = item_name
        pipeline_log_stream.put(p_worker_status_message(None, None, None, p_worker_status.USER_MESSAGE, log_level=None, message=dict))
                