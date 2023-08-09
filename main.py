import logging
import pymysql
from pydantic import BaseModel
from fastapi import FastAPI, Query

# API
app = FastAPI()

# Logging
logging.basicConfig(level=logging.INFO)
file_handler = logging.FileHandler("api.log")
file_handler.setLevel(logging.INFO)
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.addHandler(file_handler)

# Configuration for the MySQL database
db_config = {
    "host": "REPLACE_ME",
    "user": "REPLACE_ME",
    "password": "REPLACE_ME",
    "database": "REPLACE_ME",
}


# Model
class RequestBody(BaseModel):
    sql_query: str


# POST method
@app.post("/sql/exec-statement")
def execute_sql_statement(request_body: RequestBody):
    logger.info("execute_sql_statement")
    logger.info(f"{request_body}")  # for debugging

    # execute SQL and return result
    result = execute_sql(request_body.sql_query)
    return {"result": result}


# Function to execute the SQL query and return the result as JSON
def execute_sql(query: str):
    try:
        # Connect to the MySQL database
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Execute the SQL query
        cursor.execute(query)

        # Fetch all the rows
        result = cursor.fetchall()

        # Convert the result to a list of dictionaries
        keys = cursor.description
        column_names = [column[0] for column in keys]
        data = [dict(zip(column_names, row)) for row in result]

        return data
    except Exception as e:
        logger.error(str(e))
        return {"error": str(e)}
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()
