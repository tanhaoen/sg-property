# from venv import create
# import mysql.connector
# from mysql.connector import errorcode
import pandas as pd

from sqlalchemy import create_engine, delete

# Obtain connection string information from the portal

class AzureConn:
    
    # client_flags = [mysql.connector.ClientFlag.SSL]
    
    def __init__(self,
    database,
    servername='nathaniel-tan-mysql',
    user='nathaniel',
    password='PcPZw64^'):
        self.config = f"mysql+mysqlconnector://{user}:{password}@{servername}.mysql.database.azure.com:3306/{database}"
        print(self.config)
        self.init_connect()
    
    def init_connect(self):
        self.conn = create_engine(self.config)
        # try:
        #     self.conn = mysql.connector.connect(**self.config)
        #     print("Connection established")
        # except mysql.connector.Error as err:
        #     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        #         print("Something is wrong with the user name or password")
        #     elif err.errno == errorcode.ER_BAD_DB_ERROR:
        #         print("Database does not exist")
        #     else:
        #         print(err)
        # else:
        #     self.cursor = self.conn.cursor()

    def query(self, querystr):
        query_frame = pd.read_sql(querystr, self.conn)
        return query_frame

    def df_to_table(self, data, tblname, **kwargs):
        try:
            data.to_sql(tblname, con=self.conn, if_exists='append', index=False, **kwargs)
            print(f"Data added to table {tblname}")
        except Exception:
            print("Error occurred while adding data")

    def delete(self, tblname, whereclause):
        sql = f"DELETE FROM {tblname} WHERE {whereclause}"
        self.conn.execute(sql)
        self.conn.commit()
        print(self.cursor.rowcount, "record(s) deleted from table", tblname)