import os
import argparse
import logging
from time import time, sleep

import src.cmaas_io as io
import src.utils as utils
from src.vertical_processing_pipeline import vertical_processing_pipeline, vertical_pipeline_step, parameter_data_stream

LOGGER_NAME = 'CMASS_MULE'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '2' # tf log level, 2 is error only

def parse_command_line():
    from typing import List
    def parse_data(path: str) -> List[str]:
        """Command line argument parser for --data. --data should accept a list of file and/or directory paths as an
           input. This function is run called on each individual element of that list and checks if the path is valid
           and if the path is a directory expands it to all the valid files paths inside the dir. Returns a list of 
           valid files. This is intended to be used in conjunction with the post_parse_data function"""
        # Check if it exists
        if not os.path.exists(path):
            msg = f'Invalid path "{path}" specified : Path does not exist'
            #log.warning(msg)
            return None
            #raise argparse.ArgumentTypeError(msg+'\n')
        # Check if its a directory
        if os.path.isdir(path):
            data_files = [os.path.join(path, f) for f in os.listdir(path) if f.endswith('.tif')]
            #if len(data_files) == 0:
                #log.warning(f'Invalid path "{path}" specified : Directory does not contain any .tif files')
        if os.path.isfile(path):
            data_files = [path]
        return data_files

    def post_parse_data(data : List[List[str]]) -> List[str]:
        """Cleans up the output of parse data from a list of lists to a single list and does validity checks for the 
           data as a whole. Returns a list of valid files. Raises an argument exception if no valid files were given"""
        # Check that there is at least 1 valid map to run on
        data_files = [file for sublist in data if sublist is not None for file in sublist]
        if len(data_files) == 0:
            msg = f'No valid files where given to --data argument. --data should be given a path or paths to file(s) \
                    and/or directory(s) containing the data to perform inference on. program will only run on .tif files'
            raise argparse.ArgumentTypeError(msg)
        return data_files
    
    def parse_directory(path : str) -> str:
        """Command line argument parser for directory path arguments. Raises argument error if the path does not exist
           or if it is not a valid directory. Returns directory path"""
        # Check if it exists
        if not os.path.exists(path):
            msg = f'Invalid path "{path}" specified : Path does not exist\n'
            raise argparse.ArgumentTypeError(msg)
        # Check if its a directory
        if not os.path.isdir(path):
            msg = f'Invalid path "{path}" specified : Path is not a directory\n'
            raise argparse.ArgumentTypeError(msg)
        return path

    parser = argparse.ArgumentParser(description='', add_help=False)
    # Required Arguments
    required_args = parser.add_argument_group('required arguments', 'These are the arguments the pipeline requires to \
                                            run')
    required_args.add_argument('--data', 
                    type=parse_data,
                    required=True,
                    nargs='+',
                    help='Path to file(s) and/or directory(s) containing the data to perform inference on. The \
                            program will run inference on any .tif files.')
    # Optional Arguments
    optional_args = parser.add_argument_group('optional arguments', '')
    
    optional_args.add_argument('--output',
                    default='results',
                    help='Directory to write the output files to, Defaults to "results"')
    optional_args.add_argument('--feedback',
                    default=None,
                    help='Optional directory to save debugging feedback on the pipeline.')
    optional_args.add_argument('--validation',
                    type=parse_directory,
                    default=None,
                    help='Optional directory containing the true segmentations. If option is provided, the pipeline \
                          will perform the validation step (Scoring the results of predictions) with this data.')   
    optional_args.add_argument('--log',
                    default='logs/Latest.log',
                    help='Option to set the file logging will output to. Defaults to "logs/Latest.log"')
    # Optional Arguments
    optional_args.add_argument('--layouts',
                    type=parse_directory,
                    required=True,
                    help='Directory containing the layout files to use. Temporarily need this until we have the layout code')
    optional_args.add_argument('--legends',
                    type=parse_directory,
                    default=None,
                    help='Directory containing the legend files to use. Temporarily need this until we have the legend code')
    # Flags
    flag_group = parser.add_argument_group('Flags', '')
    flag_group.add_argument('-h', '--help',
                    action='help', 
                    help='show this message and exit')
    flag_group.add_argument('-v', '--verbose',
                    action='store_true',
                    help='Flag to change the logging level from INFO to DEBUG')
    args = parser.parse_args()
    args.data = post_parse_data(args.data)
    return args

from src.mysteps import load_map_wrapper, add_precomputed_layout_data, add_precomputed_legend_data, load_pattern_model, classify_legend_pattern

def main():
    main_stime = time()
    args = parse_command_line()

    # Start logger
    global log
    console_log_level = logging.DEBUG if args.verbose else logging.INFO
    log = utils.start_logger(LOGGER_NAME, args.log, log_level=logging.DEBUG, console_log_level=console_log_level)
    
    # Log startup statment
    log.info(f'Running pipeline on {os.uname()[1]} with following parameters:\n' +
            f'\tData         : {args.data}\n' +
            f'\tValidation   : {args.validation}\n' +
            f'\tOutput       : {args.output}\n' +
            f'\tFeedback     : {args.feedback}')

    # Format the data
    map_names = [os.path.splitext(os.path.basename(f))[0] for f in args.data]

    log.info(f'preloading models')
    pmodel = load_pattern_model('/projects/bbym/nathanj/attentionUnet/pattern/pattern_clasification_model.hdf5')

    # Construct pipeline
    p = vertical_processing_pipeline()
    p.workers = 4
    p._monitor.timeout = 2
    # 1 Step Load
    p.add_step(func=load_map_wrapper, args=(parameter_data_stream(args.data, names=map_names),), name='Loading Image')
    if args.layouts is not None:
        layout_files = [os.path.join(args.layouts, os.path.splitext(os.path.basename(f))[0] + '.json') for f in args.data]
        p.add_step(func=add_precomputed_layout_data, args=(p.steps[0].output(), parameter_data_stream(layout_files, names=map_names)), name='Loading Uncharted Layout')
    if args.legends is not None:
        legend_files = [os.path.join(args.legends, os.path.splitext(os.path.basename(f))[0] + '.json') for f in args.data]
        p.add_step(func=add_precomputed_legend_data, args=(p.steps[1].output(), parameter_data_stream(legend_files, names=map_names)), name='Loading UGSG Legend')
    #p.add_step(func=classify_legend_pattern, args=(p.steps[2].output(), pmodel), name='Classifying Pattern')

    # 2 Step Load
    #p.add_step(func=load_map_wrapper, args=(parameter_data_stream(args.data, names=map_names),), name='Loading Image')
    #p.add_step(func=add_layout_data, args=(p['Loading Image'].output(), parameter_data_stream(layout_files, names=map_names)), name='Loading Layout')
    

    # Some ideas of what other steps will be added
    # idea is that each step passes the map data object to the next step
    # p.add_step(pipeline_step(func=gen_layout, args=(p.step[0].output(),), name='Layout')) 
    # p.add_step(pipeline_step(func=extract_text, args=(p.step[1].output(),), name='Georeferencing'))
    # p.add_step(pipeline_step(func=extract_map_units, args=(p.step[2].output(),), name='Extracting map units'))
    # p.add_step(pipeline_step(func=map_unit_pattern_match, args=(p.step[3].output(),), name='Map Unit Pattern Matching'))

    # p.add_step(pipeline_step(func=georeference, args=(p.step[4].output(),), name='Georeferencing'))
    # p.add_step(pipeline_step(func=get_map_metadata, args=(p.step[5].output(),), name='Extracting Metadata'))

    # p.add_step(pipeline_step(func=save_output, args=(p.step[6].output(),), name='Saving'))
    # if args.validation is not None:
    #    p.add_step(pipeline_step(func=validation, args=(p.step[6].output(),), name='Validation'))

    # Run pipeline
    log.warning(f'Starting pipeline with {len(p.steps)} steps')
    p.start()
    p.monitor()

    # Log time taken
    main_etime = time()
    log.info(f'Pipeline took {main_etime - main_stime} seconds to run')

if __name__ == '__main__':
    main()
