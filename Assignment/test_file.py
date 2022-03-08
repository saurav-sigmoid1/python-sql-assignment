import unittest
from unittest import mock
import psycopg2
import datetime
import logging
logging.basicConfig(level=logging.INFO)
from Assignment.code import workOnDatabase
obj = workOnDatabase()

class MyTestCase(unittest.TestCase):

    def setUp(self):
        """connect to database"""
        self.conn = psycopg2.connect(dbname="python-sql",
                        user="postgres",
                        host="localhost",
                        password="admin",
                        port="5432")
        logging.info("this is setup ")

    def tearDown(self):
        """Close the database"""
        self.conn.close()
        logging.info("this is teardown ")

    @mock.patch("Assignment.code.workOnDatabase.connect",return_value="connect")
    def test_connection(self,abc):
        val = obj.connect()
        self.assertEqual(val, "connect")

    @mock.patch("Assignment.code.workOnDatabase.read_emp_detail", return_value="emp_detail")
    def test_read_emp_details(self, abc):
        val = obj.read_emp_detail("abc")
        self.assertEqual(val, "emp_detail")

    @mock.patch("Assignment.code.workOnDatabase.calculate_compensation", return_value="compensation")
    def test_calculate_compensation(self, abc):
        val = obj.calculate_compensation("abc")
        self.assertEqual(val, "compensation")

    @mock.patch("Assignment.code.workOnDatabase.insert_data_into_new_table", return_value="insertdata")
    def test_insert_data_in_new_table(self,abc):
        val = obj.insert_data_into_new_table()
        self.assertEqual(val,"insertdata")

    @mock.patch("Assignment.code.workOnDatabase.insert_data_in_new_table_at_department_level", return_value="department")
    def test_insert_data_at_department_level(self,abc):
        val = obj.insert_data_in_new_table_at_department_level()
        self.assertEqual(val,"department")

    def test_query(self):
        test_row = (7369, 'SMITH', 'CLERK', 7902, datetime.date(1980, 12, 17), 800.00, None, 20)
        cursor = self.conn.cursor()
        cursor.execute("select * from emp")
        row = cursor.fetchone()
        self.assertEqual(row,test_row)





if __name__ == '__main__':
    unittest.main()
