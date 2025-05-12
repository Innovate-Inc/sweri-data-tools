import logging
import watchtower

# logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8',
                    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S', force=True)
# logger.addHandler(watchtower.CloudWatchLogHandler())

def log_this(func):
    def wrapper(*args, **kwargs):
        logging.info(f"Starting: {func.__name__} with {args} {kwargs}")
        result = func(*args, **kwargs)
        logging.info(f"Finished: {func.__name__}")
        return result
    return wrapper
