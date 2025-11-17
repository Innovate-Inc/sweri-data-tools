import logging
import sys

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', encoding='utf-8',
                    level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S', force=True)
try:
    import watchtower
    logger.addHandler(watchtower.CloudWatchLogHandler())
except Exception as e:
    logger.warning(f'watchtower not available: {e}')

stream_handler = logging.StreamHandler(sys.stdout)
logger.addHandler(stream_handler)
def log_this(func):
    def wrapper(*args, **kwargs):
        logger.info(f"Starting: {func.__name__} with {args} {kwargs}")
        result = func(*args, **kwargs)
        logger.info(f"Finished: {func.__name__}")
        return result
    return wrapper
