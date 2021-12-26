import re
import os
import shutil
from Application_Logger.Logger import Log
import json
import pandas as pd
from datetime import datetime

class Raw_Validation:
    def __init__(self,data_path):
        self.batch_directory=data_path
        self.schema='schema_training.json'
        self.Log=Log()


    # Getting Value From Schema
    def ValueFromSchema(self):
        file = open('Training_Log_Details/ValueFromSchemaValidation.txt', 'a+')
        try:
            self.Log.log(file, 'Enter in ValueFromSchema method of Raw_Validation Class. Start Reading Value from Schema')
            with open(self.schema, 'r') as f:
                dic=json.load(f)
            f.close()
            datestamp=dic['LengthOfDateStampInFile']
            timestamp=dic['LengthOfTimeStampInFile']
            numcol=dic['NumberofColumns']
            colname=dic['ColName']
            self.Log.log(file,'Successfully Read Value from Schema. Exit from ValueFromSchema method of Raw_Validation Class')
            file.close()
            return datestamp, timestamp, numcol, colname

        except KeyError as e:
            self.Log.log(file, 'Getting KeyError error while Read Value from Schema in ValueFromSchema method of Raw_Validation Class. Error Msg: '+str(e))
            file.close()
            raise e

        except ValueError as e:
            self.Log.log(file, 'Getting ValueError error while Read Value from Schema in ValueFromSchema method of Raw_Validation Class. Error Msg: '+str(e))
            file.close()
            raise e

        except Exception as e:
            self.Log.log(file, 'Getting an error while Read Value from Schema in ValueFromSchema method of Raw_Validation Class. Error Msg: '+str(e))
            file.close()
            raise e

    def mutual_regex(self):
        regex="['FlightPrice']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def DeletExistingGoodData(self):
        file = open('Training_Log_Details/General_Log.txt', 'a+')
        try:
            self.Log.log(file,'Enter in DeletExistingGoodData method of Raw_Validation Class. Start Deleting Existing Good Data Folder')

            path = 'Training_Raw_Data_Validated/'
            if os.path.isdir(path+'Good_Data/'):
                shutil.rmtree(path+'Good_Data/')
            self.Log.log(file,'Deleted Existing Good Data Folder. Exit from DeletExistingGoodData method of Raw_Validation Class')
            file.close()
        except Exception as e:
            self.Log.log(file,'Error Occoured while Deleteing Existing Good Data Folder in DeletExistingGoodData method of Raw_Validation Class. Error Msg: '+str(e))
            file.close()
            raise e

    def DeleteExistingBadData(self):
        file=open('Training_Log_Details/General_Log.txt', 'a+')
        try:
            self.Log.log(file, 'Enter in DeleteExistingBadData method of Raw_Validation Class. Start Existing Deleting Bad Data Folder')
            path='Training_Raw_Data_Validated/'
            if os.path.isdir(path+'Bad_Data/'):
                shutil.rmtree(path+'Bad_Data/')
            self.Log.log(file, 'Deleted Existing Bad Data Folder. Exit from DeleteExistingBadData method of Raw_Validation Class')
            file.close()
        except Exception as e:
            self.Log.log(file, 'Error Occoured while Deleteing Existing Bad Data Folder in DeletExistingBadData method of Raw_Validation Class. Error Msg: '+str(e))
            file.close()
            raise e


    def CreateGoodBadData(self):
        file=open('Training_Log_Details/General_Log.txt', 'a+')
        path_1=os.path.join('Training_Raw_File_Validated/', 'Bad_Data/')
        path_2= os.path.join('Training_Raw_File_Validated/', 'Good_Data/')
        try:
            self.Log.log(file, 'Enter in CreateGoodBadData method of Raw_Validation Class. Start Creating Good and Bad Data Folder')
            if not os.path.isdir(path_1):
                os.mkdir(path_1)
            self.Log.log(file, 'Created new Bad Data Folder')

            if not os.path.isdir(path_2):
                os.mkdir(path_2)
            self.Log.log(file, 'Created new Good Data Folder')
            self.Log.log(file, 'Successfully Created new Good and Bad Data Folder.Exit from CreateGoodBadData method of Raw_Validation Class')
            file.close()
        except Exception as e:
            self.Log.log(file, 'Error Occoured while Creating Good and Bad Data Folder in CreateGoodBadData method of Raw_Validation Class. Error Msg: '+str(e))
            file.close()
            raise e

    def FileNameValidation(self,regex,datestamp,timestamp):
        self.DeleteExistingBadData()
        self.DeletExistingGoodData()
        self.CreateGoodBadData()
        try:
            file_1 = open('Training_Log_Details/FileNameValidation.txt', 'a+')
            self.Log.log(file_1, 'Enter in FileNameValidation method of Raw_Validation Class. File Name Validation Started')
            files=[f for f in os.listdir(self.batch_directory)]
            for file in files:
                if re.match(regex,file):
                    splitAt=re.split('.csv',file)
                    splitAt=(re.split('_', splitAt[0]))
                    if len(splitAt[1])==datestamp:
                        if len(splitAt[2])==timestamp:
                            shutil.copy('Training_Batch_Files/'+file, 'Training_Raw_File_Validated/Good_Data')
                            self.Log.log(file_1,'File Name validated move into Temporary Good Data Folder!!! %s '%file)
                        else:
                            shutil.copy('Training_Batch_Files/' + file, 'Training_Raw_File_Validated/Bad_Data')
                            self.Log.log(file_1, 'Invalid File Name move into Bad Data Folder!!! %s ' % file)
                    else:
                        shutil.copy('Training_Batch_Files/' + file, 'Training_Raw_File_Validated/Bad_Data')
                        self.Log.log(file_1, 'Invalid File Name move into Bad Data Folder!!! %s ' % file)
                else:
                    shutil.copy('Training_Batch_Files/' + file, 'Training_Raw_File_Validated/Bad_Data')
                    self.Log.log(file_1, 'Invalid File Name move into Bad Data Folder!!! %s ' % file)
            self.Log.log(file_1, 'File Name Validation Completed. Exit from FileNameValidation method of Raw_Validation Class')
            file_1.close()
        except Exception as e:
            file_1 = open('Training_Log_Details/General_Log.txt', 'a+')
            self.Log.log(file_1, 'Getting an Error while Validating File Name in FileNameValidation method of Raw_Validation Class. Error Msg: '+str(e))
            file_1.close()
            raise e

    def ColumnLengthValidation(self,colnum):
        try:
            f=open('Training_Log_Details/FileColumnLengthValidation.txt', 'a+')
            self.Log.log(f,'Enter in ColumnLengthValidation method of Raw_Validation Class. File Column Length validation Started')
            for file in os.listdir('Training_Raw_File_Validated/Good_Data/'):
                csv=pd.read_csv('Training_Raw_File_Validated/Good_Data/'+file)
                if csv.shape[1] == colnum:
                    pass
                else:
                    shutil.move('Training_Raw_File_Validated/Temp_Good_Data/'+file, 'Training_Raw_File_Validated/Bad_Data/')
                    self.Log.log(f, '%s Column Length is not Equal. File store in Bad Data Folder'%file)
            self.Log.log(f, 'File Column Length validation Completed. Exit from ColumnLengthValidation method of Raw_Validation Class')
            f.close()
        except Exception as e:
            f = open('Training_Log_Details/FileLengthValidation.txt', 'a+')
            self.Log.log(f, 'Getting an error while perform Column Length Validation operation in ColumnLengthValidation method of Raw_Validation Class. Error Msg: '+str(e))
            f.close()
            raise e


    def MissingValueInWholeColumnValidation(self):
        try:
            f=open('Training_Log_Details/MissingValueInWholeColum.txt','a+')
            self.Log.log(f,'Enter in MissingValueInWholeColumnValidation method of Raw_Validation Class. Start Validation to Find Any Column entirely Contain Missing Value')
            for file in os.listdir('Training_Raw_File_Validated/Good_Data/'):
                csv=pd.read_csv('Training_Raw_File_Validated/Good_Data/' +file)
                for col in csv:
                    if (len(csv[col]) - csv[col].count()) == len(csv[col]):
                        shutil.move('Training_Raw_File_Validated/Good_Data/' +file,
                                    'Training_Raw_File_Validated/Bad_Data')
                        self.Log.log(f,str(file)+' file Contain a Column have all Null value, File move into bad data folder')
            self.Log.log(f, 'Completed Validation to Find Any Column entirely Contain Missing Value. Exit from MissingValueInWholeColumnValidation method of Raw_Validation Class')
            f.close()

        except Exception as e:
            f = open('Training_Log_Details/MissingValueInWholeColum.txt', 'a+')
            self.Log.log(f, 'Error Occoured while Checking any Column Contain with entire Null value in MissingValueInWholeColumnValidation method of Raw_Validation Class. Error Msg: '+str(e))
            f.close()
            raise Exception

    def MoveBadArchiveFolder(self):
        file1=open('Training_Log_Details/MoveBadArchiveFolder.txt','a+')
        file_2 = open('Training_Log_Details/General_Log.txt', 'a+')
        self.Log.log(file1, 'Enter in MoveBadArchiveFolder method of Raw_Validation Class. Start Bad Data moving into Archive Folder')
        now=datetime.now()
        date=now.date()
        time=now.strftime('%H%M%S')
        try:
            source='Training_Raw_File_Validated/Bad_Data/'
            if os.path.isdir(source):
                temp='TrainingBadArchiveFolder'
                if not os.path.isdir(temp):
                    os.mkdir(temp)
            dest = 'TrainingBadArchiveFolder/Bad_Data_'+str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.mkdir(dest)
            files=os.listdir(source)
            for file in files:
                if file not in os.listdir(dest):
                    shutil.move(source+file,dest)
            self.Log.log(file1, 'Bad Data move into Archive Folder')
            path='Training_Raw_File_Validated/'
            if os.path.isdir(path + 'Bad_Data/'):
                shutil.rmtree(path + 'Bad_Data/')
            self.Log.log(file_2,'Bad Data folder file Successfully move into Archive Folder.Exit from MoveBadArchiveFolder method of Raw_Validation Class')
            file_2.close()
            file1.close()

        except Exception as e:
            self.Log.log(file1, 'Error Occoured while File Move from bad data to Archive folder in MissingValueInWholeColumnValidation method of Raw_Validation Class. Error Msg: '+str(e))
            file1.close()
            file_2.close()
            raise e
