import psycopg2
import pandas as pd 
from sqlalchemy import engine, create_engine, text
import logging

class wrapper:
    logging.getLogger().addHandler(logging.StreamHandler())
    _engine = None
    _schema = {}
    _relation = {}
 
    _supported_services= ['postgres']

    def __init__(self, user: str, password: str, url: str, port: int, database: str, service: str):

        if service == "postgres":
            engine_params = "postgresql+psycopg2://"+user+":"+password+"@"+url+":"+str(port)+"/"+database
            self._engine = create_engine(engine_params)
            with self._engine.connect() as conn:
                logging.info("database "+database+" connected")

                #Get database schema
                query = conn.execute(text("SELECT distinct table_schema FROM information_schema.columns"))
                schemas = query.fetchall()
                print(schemas)
                for schema in schemas:
                    print(schema[0])
                    query = conn.execute(text("SELECT distinct(table_name) FROM information_schema.columns WHERE table_schema like '"+schema[0]+"'"))
                    tables = query.fetchall()
                    self._schema[schema[0]] = {}
                    for table in tables:
                        query = conn.execute(text("SELECT distinct(column_name) FROM information_schema.columns	WHERE table_schema like '"+schema[0]+"' AND table_name like '"+table[0]+"';"))
                        columns = query.fetchall()
                        self._schema[schema[0]][table[0]] = []
                        for column in columns:
                            self._schema[schema[0]][table[0]].append(column[0])

                #Get database relations
                query = conn.execute("SELECT distinct concat(tc.table_schema,'.', tc.table_name) as table_name FROM information_schema.table_constraints AS tc WHERE tc.constraint_type = 'FOREIGN KEY'")
        else:
            raise ValueError("unsupported database")

        print(self._schema)
        print(self._relation)
        
        

    
        


