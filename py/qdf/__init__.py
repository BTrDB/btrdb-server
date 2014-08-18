from quasar import *
from qdf import *

OPTIMAL_BATCH_SIZE = 100000
MICROSECOND = 1000
MILLISECOND = 1000*MICROSECOND
SECOND      = 1000*MILLISECOND
MINUTE      = 60*SECOND
HOUR        = 60*MINUTE
DAY         = 24*HOUR
MIN_TIME    = -(16<<56)
MAX_TIME    = (48<<56)
LATEST      = 0

_all__ = ["connectToArchiver", "register", "begin", "QuasarDistillate"]
