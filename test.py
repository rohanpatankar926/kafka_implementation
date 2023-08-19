from src.kafka_logger.logger import logging


def div(a,b):
    try:
        logging.info("add function called")
        add=a/b
        logging.info(f"addition of {a} and {b} is {add}")
        return add
    except Exception as e:
        logging.error(f"Something wrong happend {str(e)}")

div(3,0)