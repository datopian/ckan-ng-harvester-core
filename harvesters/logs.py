import logging
logger = logging.getLogger(__name__)

c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('harvest.log')

f_handler.setLevel(logging.DEBUG)
c_handler.setLevel(logging.INFO)

logger.addHandler(c_handler)
logger.addHandler(f_handler)

logger.setLevel(logging.DEBUG)
