# doing all necessary import

import csv
import os
import shutil
import sqlite3
from Application_Logger.Logger import Log


class DBOperation:

    """
          This class shall be used for handling all the SQL operations.

                                     Written By: Abhishek Saha
                                     Version: 1.0
                                     Revisions: None
    """

    def __init__(self):
        self.good_data='Prediction_Raw_File_Validated/Good_Data'
        self.bad_data='Prediction_Raw_File_Validated/Bad_Data'
        self.db_path='Prediction_db/'
        self.log=Log()

    def dbconnection(self, databasename):

        """
                                      Method Name: dbconnection
                                      Description: This method create the database with the given name and if
                                                   Database already exists then opens the connection and return
                                                   the connection object.
                                      Output: Connection to the DB
                                      On Failure: Raise ConnectionError


                                      Written By: Abhishek Saha
                                      Version: 1.0
                                      Revisions: None
        """

        try:
            file_1=open('Prediction_Log_Details/DBConnection_Log.txt','a+')
            self.log.log(file_1, 'Start Database Connectivity.')
            conn=sqlite3.connect(self.db_path+databasename+'.db')
            file_1.close()
            return conn
        except ConnectionError as e:
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            self.log.log(file_1, 'Getting an error while build connection with Database. In dbConnection method of DBOperation Class. Error msg: '+str(e))
            file_1.close()
            raise e


    def createTable(self, databasename, colname):

        """
                                        Method Name: createTable
                                        Description: This method creates a table in the given database which will be used
                                                     to insert the Good data after raw data validation.
                                        Output: None
                                        On Failure: Raise Exception


                                        Written By: Abhishek Saha
                                        Version: 1.0
                                        Revisions: None
        """

        try:
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/CreateTable_Log.txt', 'a+')
            conn=self.dbconnection(databasename)
            self.log.log(file_2, 'Enter in createTable method of DBOperation Class. Start Table Creation')
            self.log.log(file_1, 'Database connected Successfully for createTable method of DBOperation Class')
            conn.execute('drop table if exists Good_Raw_Data')
            self.log.log(file_2, 'Drop existing table')

            for key in colname.keys():
                type = colname[key]
                try:
                    conn.execute('alter table Good_Raw_Data add COLUMN "{name}" {datatype}'.format(name=key, datatype=type))
                except:
                    conn.execute('create table Good_Raw_Data ({name} {datatype})'.format(name=key, datatype=type))
            conn.close()
            self.log.log(file_1, 'Database connection Closed Successfully')
            self.log.log(file_2, 'New Table Created Successfully. Exit from createTable method of DBOperation Class')
            file_1.close()
            file_2.close()
        except Exception as e:
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/CreateTable_Log.txt', 'a+')
            conn.close()
            self.log.log(file_1, 'Database connection Closed')
            self.log.log(file_2, 'Getting an error while Create a new table in createTable method of DBOperation Class. Error Msg: '+str(e))
            file_1.close()
            file_2.close()
            raise e

    def insertIntodb(self, databasename):

        """
                                      Method Name: insertIntodb
                                      Description: This method inserts the Good data files from the Good_Data
                                                   Directory into database table.
                                      Output: None
                                      On Failure: Raise Exception


                                      Written By: Abhishek Saha
                                      Version: 1.0
                                      Revisions: None
        """

        good_data=self.good_data
        bad_data=self.bad_data
        try:
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/InsertintoDB_Log.txt', 'a+')
            conn=self.dbconnection(databasename)
            self.log.log(file_1, 'Database connected Successfully for insertIntodb method')
            self.log.log(file_2, 'Enter in insertIntodb method of DBOperation Class. Start Data inserting into DataBase')
            files=[f for f in os.listdir(good_data)]
            for file in files:
                with open(good_data+"/"+file, 'r') as f:
                    next(f)
                    read=csv.reader(f, delimiter='\n')
                    for lst1 in enumerate(read):
                        for lst_ in lst1[1]:
                            try:
                                conn.execute('Insert into Good_Raw_Data values ({value})'.format(value=lst_))
                                conn.commit()
                            except Exception as e:
                                raise e
                    self.log.log(file_2,'File: '+str(file)+' Insert into Database')
            conn.close()
            self.log.log(file_1, 'Database connection Closed')
            self.log.log(file_2, 'File insertion in Database done. Exit from insertIntodb method of DBOperation Class.')
            file_1.close()
            file_2.close()
        except Exception as e:
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/InsertintoDB_Log.txt', 'a+')
            conn.rollback()
            self.log.log(file_2, 'Error Occurred while data insert into table in insertIntodb method of DBOperation Class. Error Msg: '+str(e))
            shutil.move(good_data + "/" + file, bad_data)
            self.log.log(file_2, 'Bad data move into Bad data folder. Issue File Name'+str(file))
            conn.close()
            self.log.log(file_1, 'Database Connection Closed')
            file_1.close()
            file_2.close()
            raise e


    def tabletoCSV(self,databasename):

        """
                                      Method Name: tabletoCSV
                                      Description: This method exports the data from Good_Raw_Data table
                                                   into a CSV file in a given location.
                                      Output: None
                                      On Failure: Raise Exception


                                      Written By: Abhishek Saha
                                      Version: 1.0
                                      Revisions: None
        """

        try:
            self.filefromDb='Prediction_File_From_DB/'
            self.filename='InputFile.csv'
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/InsertintoCSV_Log.txt', 'a+')
            conn=self.dbconnection(databasename)
            self.log.log(file_1,'Database Connected Successfully for tabletoCSV method')
            self.log.log(file_2, 'Enter in tabletoCSV method of DBOperation Class. Start Data load into CSV file')
            if not os.path.isdir(self.filefromDb):
                os.mkdir(self.filefromDb)
            cur=conn.cursor()
            cur.execute('select * from Good_Raw_Data')
            results=cur.fetchall()
            header=[i[0] for i in cur.description]
            csv_file=csv.writer(open(self.filefromDb+self.filename, 'w', newline=''), delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')
            csv_file.writerow(header)
            csv_file.writerows(results)
            conn.close()
            self.log.log(file_1, 'Database Connection Closed')
            self.log.log(file_2, 'Successfully load into CSV file. Exit from tabletoCSV method of DBOperation Class')
            file_1.close()
            file_2.close()

        except Exception as e:
            file_1 = open('Prediction_Log_Details/DBConnection_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/InsertintoCSV_Log.txt', 'a+')
            self.log.log(file_2, 'Error Occurred while data load into CSV file in tabletoCSV method of DBOperation Class. Error Msg: '+str(e))
            conn.close()
            self.log.log(file_1, 'Database Connection Closed')
            file_1.close()
            file_2.close()
            raise e
