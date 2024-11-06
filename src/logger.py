import logging
import os
import sys
from datetime import datetime



#define the logfile name using the current datae and time

log_filename = f'{datetime.now().strftime('%m_%d_%y_%H_%M_%S')}.log'

# create the path to the log file
log_filepath = os.path.join(os.getcwd(),'logs',log_filename)

# create the log directory if it doesn't exist
os.makedirs(os.path.dirname(log_filename),exist_ok=True)

# logger configuration

logging.basicConfig(
    format= '[%(asctime)s ] %(lineno)d %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler(log_filepath), # Outputs logs to the file
        logging.StreamHandler(sys.stdout) # Output logs in the terminal
    ]
)

logger = logging.getLogger('src')