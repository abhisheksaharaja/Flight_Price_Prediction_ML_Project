#doing all necessary import

import csv
import os
import shutil
import sqlite3
from Application_Logger.Logger import Log

class DBOperation:

    """
               This class shall be used for handling all the SQL operations.
    """

    def __init__(self):
        self.db_path='Training_db/'
        self.good_data='Training_Raw_File_Validated/Good_Data'
        self.bad_data='Training_Raw_File_Validated/Bad_Data'
        self.logger=Log()

    def createTable(self, Databasename,colname):

        """
                                        Method Name: createTable
                                        Description: This method creates a table in the given database which
                                                     will be used to insert the Good data Directory after
                                                     raw data validation.
                                        Output: None
                                        On Failure: Raise Exception
        """

        file_1 = open('Training_Log_Details/DBConnection.txt', 'a+')
        file_2 = open('Training_Log_Details/CreateTable.txt', 'a+')
        try:
            self.logger.log(file_2, 'Enter in createTable method of DBOperation Class. Start Table Creation')
            conn=self.CreateConnection(Databasename)
            self.logger.log(file_1, 'Database Connected for createTable method')
            cur=conn.cursor()
            cur.execute("select count(name) from sqlite_master where type='table' and name='Good_Raw_Data'")
            if cur.fetchone()[0]==1:
                conn.close()
                self.logger.log(file_2, 'Table Already Created')
                self.logger.log(file_1, 'Database Connection Closed')

            else:
                for key in colname.keys():
                    type=colname[key]
                    try:
                       conn.execute('alter table Good_Raw_Data add COLUMN "{name}" {datatype}'.format(name=key, datatype=type))
                    except:
                        conn.execute('create table Good_Raw_Data ({name} {datatype})'.format(name=key, datatype=type))
                conn.close()
                self.logger.log(file_1, 'Database Connection Closed')
                self.logger.log(file_2, 'Table Created Successfully. Exit from createTable method of DBOperation Class')
            file_1.close()
            file_2.close()
        except Exception as e:
            conn.close()
            self.logger.log(file_2, 'Getting an Error while Creating table in createTable method of DBOperation Class. Error Msg:'+str(e))
            self.logger.log(file_1, 'Database Connection Closed. Getting an Error while Creating table in createTable method of DBOperation Class. Error Msg:'+str(e))
            file_1.close()
            file_2.close()

    def CreateConnection(self,dbname):

        """
                                       Method Name: CreateConnection
                                       Description: This method creates the database with the given name and
                                                    if Database already exists then opens the connection to the DB
                                                    and return the connection object.
                                       Output: Connection to the DB
                                       On Failure: Raise ConnectionError

        """

        f=open('Training_Log_Details/DBConnection.txt', 'a+')
        try:

            self.logger.log(f, 'Start Database Connecting Process')
            conn=sqlite3.connect(self.db_path+dbname+'.db')
            f.close()
            return conn
        except ConnectionError as e:
            self.logger.log(f, 'Error while Establish Connection with Database. Error Msg: '+str(e))
            f.close()
            raise e

    def InsertIntoDB(self,databasename):

        """
                                    Method Name: InsertIntoDB
                                    Description: This method insert the Good data files from the Good_Data
                                                 Directory into Database.
                                    Output: None
                                    On Failure: Raise Exception
        """

        file1=open('Training_Log_Details/DbInsertion.txt','a+')
        file2 = open('Training_Log_Details/DBConnection.txt', 'a+')
        goodfile=self.good_data
        badfile=self.bad_data

        try:
            conn=self.CreateConnection(databasename)
            self.logger.log(file2,'Database Connected for InsertIntoDB method')
            self.logger.log(file1, 'Enter in InsertIntoDB method of DBOperation Class. Start Data inserting into DataBase')
            files = [f for f in os.listdir(goodfile)]
            for file in files:
                with open(goodfile+'/'+file, 'r') as f:
                    next(f)
                    reader=csv.reader(f, delimiter="\n")
                    for lst1 in enumerate(reader):
                        for lst_ in lst1[1]:
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({temp})'.format(temp=lst_))
                                conn.commit()
                            except Exception as e:
                                raise e
                    self.logger.log(file1, '%s File Loaded Successfully in Database' % file)
            conn.close()
            self.logger.log(file2, 'Database Connection Closed')
            self.logger.log(file1, 'File insertion in Database done.Exit from InsertIntoDB method of DBOperation Class')
            file1.close()
            file2.close()
        except Exception as e:
            conn.rollback()
            self.logger.log(file1, 'Error Occoured while data insert into table in InsertIntoDB method of DBOperation Class. Error Msg: '+str(e))
            shutil.move(goodfile+"/"+file, badfile)
            self.logger.log(file1, 'Bad data move into Bad data folder.Error Occoured while data insert into table in InsertIntoDB method of DBOperation Class. Error Msg: '+str(e))
            conn.close()
            self.logger.log(file2, 'Database Connection Closed')
            file1.close()
            file2.close()
            raise e

    def InsertIntoCSV(self,databasename):

        """
                                    Method Name: InsertIntoCSV
                                    Description: This method fetch data from Good_Raw_Data table to a CSV
                                                 file into a specified given location.
                                    Output: None
                                    On Failure: Raise Exception
        """

        self.FileFromDB='Training_File_From_DB/'
        self.FileName='Input.csv'
        file1=open('Training_Log_Details/CSVLoad.txt','a+')
        file2 = open('Training_Log_Details/DBConnection.txt', 'a+')
        try:
            self.logger.log(file1,'Enter in InsertIntoCSV method of DBOperation Class. Start Data load into CSV file')
            if not os.path.isdir(self.FileFromDB):
                os.mkdir(self.FileFromDB)
            conn=self.CreateConnection(databasename)
            self.logger.log(file2, 'Database Connected for InsertIntoCSV method')
            cur=conn.cursor()
            cur.execute('select * from Good_Raw_Data')
            results=cur.fetchall()
            header=[i[0] for i in cur.description]
            csvfile=csv.writer(open(self.FileFromDB+self.FileName, 'w', newline=''), delimiter=',', lineterminator='\r\n', quoting=csv.QUOTE_ALL, escapechar='\\')
            csvfile.writerow(header)
            csvfile.writerows(results)
            self.logger.log(file1, 'Successfully data load into CSV file. Exit from InsertIntoCSV method of DBOperation Class.')
            conn.close()
            self.logger.log(file2, 'Database Connection Closed.')
            file1.close()
            file2.close()

        except Exception as e:
            self.logger.log(file1, 'Error Occurred while data load into CSV file in InsertIntoCSV method of DBOperation Class. Error Msg: '+str(e))
            conn.close()
            self.logger.log(file2, 'Database Connection Closed')
            file1.close()
            file2.close()
            raise e


