"""Drop-in cx_Oracle via python-oracledb (Python 3.12+; cx_Oracle не собирается)."""
import sys

import oracledb

sys.modules[__name__] = oracledb
