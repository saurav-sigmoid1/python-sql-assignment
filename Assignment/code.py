import psycopg2
import os
from sqlalchemy import create_engine
import pandas as pd
from config import config
import pandas.io.sql as sql
import pandasql as ps
import logging
logging.basicConfig(level=logging.INFO)

class workOnDatabase:

    def connect(self):
        """ Connect to the PostgreSQL database server """
        try:
            # read connection parameters
            params = config()
            # connect to the PostgreSQL server
            logging.info('Connecting to the PostgreSQL database...')
            conn = psycopg2.connect(**params)
            return conn
        except (Exception, psycopg2.DatabaseError) as error:
            logging.info(error)

    def close_connection(self,conn):
        """Close the database connection"""
        try:
            conn
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
                logging.info('Database connection closed.')

    def read_emp_detail(self, conn):
        """ read the employee data from emp table in postgresql and store in xlsx file"""

        # read the data from postgresql database
        emp_detail=sql.read_sql('select empno,ename,mgr from emp',conn)
        # logging.info(emp_detail)
        emp_detail_path ="/Users/sauravverma/pythonProject1/python-sql/Assignment/emp_detail.xlsx"
        if not os.path.isfile(emp_detail_path):
            emp_detail.to_excel(emp_detail_path,index=False)
        self.close_connection(conn)

    def calculate_compensation(self,conn):
        """Reading data from multiple table in postgresql and store
        that data in xlsx file after calculating total compensation"""

        emp_compensation = sql.read_sql("select e.empno,e.ename,d.dname,d.deptno,e.sal+(case when jh.comm is not NULL then jh.comm else 0 end) as total_Compensation, DATE_PART('year',AGE((case when enddate IS NULL then current_date else enddate end), startdate))*12 + DATE_PART('months',AGE((case when enddate IS NULL then current_date  else enddate end),startdate)) as months from jobhist as jh join emp as e on e.empno=jh.empno join dept as d on d.deptno=jh.deptno",conn)
        # logging.info(emp_compensation)
        emp_compensation_path = "/Users/sauravverma/pythonProject1/python-sql/Assignment/emp_compensation.xlsx"
        if not os.path.isfile(emp_compensation_path):
            emp_compensation.to_excel(emp_compensation_path, index=False)
        self.close_connection(conn)


    def insert_data_into_new_table(self, conn):
        """Reading data from xlsx file and """

        engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/python-sql')
        with pd.ExcelFile("/Users/sauravverma/pythonProject1/python-sql/Assignment/emp_compensation.xlsx") as xls:
            df = pd.read_excel(xls)
            df.to_sql(name="employee_compensation", con=engine, if_exists='append', index=False)
        self.close_connection(conn)

    def insert_data_in_new_table_at_department_level(self,conn):
        """reading data from xlsx file and manupulate data and insert in postgresql new table"""

        engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/python-sql')
        with pd.ExcelFile("/Users/sauravverma/pythonProject1/python-sql/Assignment/emp_compensation.xlsx") as xls:
            df = pd.read_excel(xls)
            new_df = ps.sqldf("select deptno,dname, sum(total_compensation) as dcompensation from df group By dname")
            new_df.to_sql(name="department_compensation", con=engine, if_exists='append', index=False)
            # print(engine.execute("SELECT * FROM department_compensation").fetchall())
        self.close_connection(conn)




if __name__ == '__main__':
    obj = workOnDatabase()
    conn = obj.connect()
    print("1.Read employee data from database\n2.Calculate total compensation from database\n3.Insert data in database\n4.Insert data into  table after manupulation ")
    val = int(input())
    if val == 1:
        obj.read_emp_detail(conn)
    elif val == 2:
        obj.calculate_compensation(conn)
    elif val == 3:
        obj.insert_data_into_new_table(conn)
    else:
        obj.insert_data_in_new_table_at_department_level(conn)

