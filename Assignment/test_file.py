import unittest
# from unittest import mock
import psycopg2
import datetime
import logging
logging.basicConfig(level=logging.INFO)
# from Assignment.code import workOnDatabase


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

    def test_query(self):
        test_row = (7369, 'SMITH', 'CLERK', 7902, datetime.date(1980, 12, 17), 800.00, None, 20)
        cursor = self.conn.cursor()
        cursor.execute("select * from emp")
        row = cursor.fetchone()
        self.assertEqual(row,test_row)





if __name__ == '__main__':
    unittest.main()
