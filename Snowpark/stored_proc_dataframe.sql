# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.
import pandas as pd
import snowflake.snowpark
import xgboost as xgb
from snowflake.snowpark.functions import sproc

@sproc(packages=["snowflake-snowpark-python", "pandas", "xgboost==1.5.0"])
def compute(session: snowflake.snowpark.Session) -> list:
    return [pd.__version__, xgb.__version__]
