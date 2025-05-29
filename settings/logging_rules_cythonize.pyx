from logging import (
  Formatter,
  getLogger,
  StreamHandler,
  DEBUG as LOGGING_DEBUG
)

# Logging Formatter
formatter = Formatter(
  # '%(asctime)s [PID:%(process)d] [TID:%(threadName)s] [%(levelname)s] [%(name)s] %(message)s',
  '%(asctime)s [PID:%(process)d] [TID:%(threadName)s] [%(levelname)s] [%(name)s] '
  '%(filename)s:%(lineno)d - %(funcName)s() - %(message)s',
  datefmt = '%Y-%m-%d %H:%M:%S'
)

logger = getLogger("Data Preparation Service")
logger.setLevel(LOGGING_DEBUG)

console_handler = StreamHandler()
console_handler.setLevel(LOGGING_DEBUG)
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)

