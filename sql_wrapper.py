import psycopg2
import pandas as pd 
from sqlalchemy import engine, create_engine, text

class wrapper:
    _engine = None
    _schema = {}
    _relation = {}
 
    _supported_services= ['postgres']

    def __init__(self, user: str, password: str, url: str, port: int, database: str, service: str):
        if service == "postgres":
            engine_params = "postgresql+psycopg2://"+user+":"+password+"@"+url+":"+str(port)+"/"+db
            log.debug(engine_params)
            self._engine = create_engine(engine_params)
            with engine.connect() as conn:
                query = conn.execute(text("SELECT distinct table_schema FROM information_schema.columns"))
                schemas = query.fetchall()
        else:
            raise ValueError("unsupported database")
        
        

    
        


