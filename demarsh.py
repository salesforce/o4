import marshal
import sys
from pprint import pprint
try:
    while True:
        pprint(marshal.load(sys.stdin))
except EOFError:
    pass
