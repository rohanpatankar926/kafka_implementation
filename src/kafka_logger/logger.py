import logging
from datetime import datetime
import os

LOG_DIR="logs"

def get_logfile_name():
    return f"log_{datetime.now().strftime('%Y-%M-%d')}.log"

LOG_FILENAME=get_logfile_name()

os.makedirs(LOG_DIR,exist_ok=True)

LOG_FILEPATH=os.path.join(LOG_DIR,LOG_FILENAME)

logging.basicConfig(filename=LOG_FILEPATH,filemode="w",level=logging.INFO)