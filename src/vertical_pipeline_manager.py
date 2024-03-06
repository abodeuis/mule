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

class pipeline_data_stream():
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

class pipeline_step():
    def __init__(self, func, args, name='', workers=1, max_output_size=5):
        self.func = func
        self.args = args
        self.name = name # Used for status messages
        self.workers = workers
        self.max_output_size = max_output_size

        # Set by pipeline manager
        self.id = None
        self._output_subscribers = []

    def output(self):
        """Returns a stream to the output of this step."""
        output_stream = pipeline_data_stream(max_size=self.max_output_size)
        self._output_subscribers.append(output_stream)
        return output_stream

def _start_worker(step, log_stream, management_stream):
    def work_ready(args):
        for arg in args:
            if isinstance(arg, pipeline_data_stream):
                if arg.empty():
                    return False
        return True
    
    def output_ready(subscribers):
        for subscriber in subscribers:
            if subscriber.full():
                return False
        return True
    pid = multiprocessing.current_process().pid
    msg = p_worker_status_message(pid, step.id, None, p_worker_status.WORKER_STARTING, log_level=logging.DEBUG, message=f'Process {pid} - Running pipeline step {step.name}')
    log_stream.put(msg)
    while True:
        try:
            # Check for stop message
            if not management_stream.empty():
                message = management_stream.get()
                if message == 'STOP':
                    msg = p_worker_status_message(pid, step.id, None, p_worker_status.WORKER_STOPPING, log_level=logging.DEBUG, message=f'Process {pid} - Ending pipeline step {step.name}')
                    log_stream.put(msg)
                    break

            # Wait for work
            if not work_ready(step.args):
                sleep(0.1)
                continue
            if not output_ready(step._output_subscribers):          
                sleep(0.1)
                continue

            

            # Retrive work from queue
            func_args = []
            arg_data = None
            for arg in step.args:
                if isinstance(arg, pipeline_data_stream):
                    arg_data = arg.get()
                    func_args.append(arg_data.data)
                else:
                    func_args.append(arg)
            
            # Run function
            msg = p_worker_status_message(pid, step.id, arg_data.id, p_worker_status.STARTED_PROCESSING, item_name=arg_data.name, log_level=None, message=f'Process {pid} - Started {step.name} : {arg_data.name}')
            log_stream.put(msg)
            result = step.func(*func_args)
            
            # Send data to subscribers
            for subscriber in step._output_subscribers:
                subscriber.append(result, id=arg_data.id, name=arg_data.name)

            msg = p_worker_status_message(pid, step.id, arg_data.id, p_worker_status.COMPLETED_PROCESSING, item_name=arg_data.name, log_level=None, message=f'Process {pid} - Completed {step.name} : {arg_data.name}')
            log_stream.put(msg)

            

        except Exception as e:
            # Log errors
            if arg_data is not None and arg_data.id:
                msg = p_worker_status_message(pid, step.id, arg_data.id, p_worker_status.ERROR, log_level=logging.ERROR, message=f'Process {pid} - Error in step {step.name} on {arg_data.name} : {e}\n{traceback.format_exc()}')
            else:
                msg = p_worker_status_message(pid, step.id, None, p_worker_status.ERROR, log_level=logging.ERROR, message=f'Process {pid} - Error in step {step.name} : {e}\n{traceback.format_exc()}')
            log_stream.put(msg)


class vertical_pipeline_manager():
    def __init__(self):
        self.steps = []
        self._workers = []
        self._running = False

        mpm = multiprocessing.Manager()
        self._log_stream = mpm.Queue()
        self._management_stream = mpm.Queue()

    def add_step(self, step):
        self.steps.append(step)
        step.id = len(self.steps) - 1

    def running(self):
        """Returns True if the pipeline is running."""
        return self._running 

    def _create_worker(self):
        w = multiprocessing.Process(target=_start_worker, args=(self.steps, self._log_stream, self._management_stream))
        w.start()
        self._workers.append(w)
    
    def start(self):
        if self._running:
            log.warning('Start was called when Inference pipeline already running. Ignoring call to start')
            return False
        
        for w in range(self._worker_count):
            self._create_worker()
        self._running = True
        log.info(f'Starting pipeline manager with {len(self.steps)} steps and {len(self._workers)} workers')
        return True
    
    def stop(self):
        if not self._running:
            log.warning('Stop was called when Inference pipeline already stopped. Ignoring call to stop')
            return False

        log.info('Stopping pipeline')
        for w in self._workers:
            self._management_stream.put('STOP')

        # Wait for workers to stop
        log.debug('Waiting for pipeline workers to finish')
        [w.join() for w in self._workers]
        self._running = False
        return True

    def monitor(self, timeout=1, refesh=0.25, max_lines=20):
        def generate_monitor_table(df:pd.DataFrame, max_lines:int=20) -> Table:
            # Bulid table structure
            table = Table(title='MULE Pipeline Monitor', expand=True, box=box.MINIMAL)
            table.add_column('Map')
            table.add_column('Shape')
            table.add_column('Map Units')
            table.add_column('Status')
            table.add_column('Processing Time')

            # Update processing time
            for index, row in df.iterrows():
                if row['step_id'] not in ['FINISHED','ERROR', None]:
                    now = time()
                    df.at[index, 'processing_time'] += now - row['last_update']
                    df.at[index, 'last_update'] = now

            # Populate table data
            total_lines = 0
            for index, row in df.iterrows():
                if total_lines >= max_lines:
                    break
                if row['step_id'] in ['FINISHED','ERROR']: # Skip completed maps
                    continue
                color = ''
                if row['step_id'] is not None:
                    color = '[green]'
                name = row['name'] if row['name'] is not None else row['item_id']
                table.add_row(f'{color}{name}', f'{color}{row["shape"]}', f'{color}{row["map_units"]}', f'{color}{step_name_lookup[row["step_id"]]}', f'{color}{row["processing_time"]:.2f} seconds')
                total_lines += 1

            # Add completed maps at the bottom
            for index, row in df[::-1].iterrows():
                if total_lines >= max_lines:
                    break
                if row['step_id'] not in ['FINISHED','ERROR']: # Skip in progress maps
                    continue
                color = '[bright_black]'
                if row['step_id'] == 'ERROR':
                    color = '[red]'
                name = row['name'] if row['name'] is not None else row['item_id']
                table.add_row(f'{color}{name}', f'{color}{row["shape"]}', f'{color}{row["map_units"]}', f'{color}{step_name_lookup[row["step_id"]]}', f'{color}{row["processing_time"]:.2f} seconds')
                total_lines += 1

            return table

        def update_monitor_df(df:pd.DataFrame, record:p_worker_status_message) -> pd.DataFrame:
            # Get row of map
            if record.item_id in df['item_id'].values:
                irow = df[df['item_id'] == record.item_id].index[0]
            else:
                irow = None

            if record.status == p_worker_status.STARTED_PROCESSING:
                if irow is not None:
                    df.at[irow, 'step_id'] = str(record.step_id)
                    df.at[irow, 'last_update'] = time()
                if record.item_id not in df['item_id'].values:
                    df.loc[len(df)] = {'item_id': record.item_id, 'step_id': str(record.step_id), 'processing_time': 0.0, 'last_update': time(), 'name': record.item_name, 'shape' : None, 'map_units' : None}
            
            elif record.status == p_worker_status.COMPLETED_PROCESSING:
                df.at[irow, 'step_id'] = None
                if record.step_id == (len(self.steps) - 1): # Last step
                    df.at[irow, 'step_id'] = 'FINISHED'

            elif record.status == p_worker_status.ERROR:
                df.at[irow, 'step_id'] = 'ERROR'
            
            return df
        
        step_name_lookup = {None : 'Waiting in queue', 'FINISHED' : 'Done processing', 'ERROR': 'ERROR'}
        for step in self.steps:
            step_name_lookup[str(step.id)] = step.name
        
        df = pd.DataFrame(columns=['item_id', 'step_id', 'processing_time', 'last_update', 'name', 'shape', 'map_units'])
        with Live(generate_monitor_table(df), refresh_per_second=(1/refesh)) as live:
            logging_handler = swap_console_handler(log, RichHandler(live))
            last_activity = time()
            while self._running:
                # Check if there is any in progress maps
                for value in df['step_id'].unique():
                    if value not in ['FINISHED', 'ERROR']:
                        last_activity = time()
                if time() - last_activity > timeout:
                     break

                # Sleep while no new messages are available
                if self._log_stream.empty():
                    live.update(generate_monitor_table(df))
                    sleep(refesh)
                    continue

                # Retieve worker messages
                record = self._log_stream.get()
                if record.log_level is not None and record.message is not None:
                    log.log(record.log_level, record.message)
                
                # Update monitor table
                df = update_monitor_df(df, record)
                live.update(generate_monitor_table(df))
            swap_console_handler(log, logging_handler)

        # Stop pipeline when done
        log.info(f'Pipeline Manager has detected that there are no more maps to process. No updates in the last {timeout} seconds')
        self.stop()
                