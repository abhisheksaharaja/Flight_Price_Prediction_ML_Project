from datetime import datetime
import re
import os
import json
import shutil
import pandas as pd
from Application_Logger.Logger import Log


class Predict_Raw_Validation:

    def __init__(self,path):
        self.log=Log()
        self.schema='schema_prediction.json'
        self.batch_directory=path

    def deletePredictFile(self):
        try:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1,'Enter in deletePredictFile method of Predict_Raw_Validation Class. Start Deleteing existing Predicted Output file')
            path_1='Predicted_Output_File/Prediction.csv'
            if os.path.exists(path_1):
                os.remove(path_1)
            self.log.log(file1,'Deleted existing Predicted Output file. Exit from deletePredictFile method of Predict_Raw_Validation Class.')
            file1.close()
            return True
        except Exception as e:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while delete Existing Predicted Output file in deletePredictFile method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            file1.close()
            raise e

    def deleteExistingGoodDataFolder(self):
        try:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Enter in deleteExistingGoodDataFolder method of Predict_Raw_Validation Class. Start Deleteing existing Good Data Folder')
            path='Prediction_Raw_File_Validated/'
            if os.path.isdir(path+'Good_Data/'):
                shutil.rmtree(path+'Good_Data/')
            self.log.log(file1, 'Deleted Existing Good Data Folder. Exit from deleteExistingGoodDataFolder method of Predict_Raw_Validation Class')
            file1.close()
        except Exception as e:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while delete Existing Good Data Folder in deleteExistingGoodDataFolder method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            file1.close()
            raise e

    def deleteExistingBadDataFolder(self):
        try:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Enter in deleteExistingBadDataFolder method of Predict_Raw_Validation Class. Start Deleteing existing Bad Data Folder')
            path='Prediction_Raw_File_Validated/'
            if os.path.isdir(path+'Bad_Data/'):
                shutil.rmtree(path+'Bad_Data/')
            self.log.log(file1, 'Deleted Existing Bad Data Folder. Exit from deleteExistingBadDataFolder method of Predict_Raw_Validation Class')
            file1.close()
        except Exception as e:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while delete Existing bad Data Folder in deleteExistingBadDataFolder method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            file1.close()
            raise e

    def createGoodBadDataFolder(self):
        try:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Enter in createGoodBadDataFolder method of Predict_Raw_Validation Class. Start Creating Good and Bad Data Folder')
            path_2 = os.path.join('Prediction_Raw_File_Validated/', 'Bad_Data/')
            path_1 = os.path.join('Prediction_Raw_File_Validated/', 'Good_Data/')
            if not os.path.isdir(path_1):
                os.mkdir(path_1)
                self.log.log(file1, 'Created Good Data Folder')
            if not os.path.isdir(path_2):
                os.mkdir(path_2)
                self.log.log(file1, 'Created Bad Data Folder')
            self.log.log(file1, 'Successfully Created Good and Bad Data Folder. Exit from createGoodBadDataFolder method of Predict_Raw_Validation Class')
            file1.close()
        except Exception as e:
            file1 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while Created Good and Bad Data Folder in createGoodBadDataFolder method of  Predict_Raw_Validation Class. Error Msg: ' + str(e))
            file1.close()
            raise e


    def fileNameValidation(self,regex,datestamplen, timestamplen):
        self.deleteExistingGoodDataFolder()
        self.deleteExistingBadDataFolder()
        self.createGoodBadDataFolder()
        try:
            file1 = open('Prediction_Log_Details/FileNameValidation.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Enter in fileNameValidation method of Predict_Raw_Validation Class. Start File name validation')
            self.log.log(file2, 'Enter in fileNameValidation method of Predict_Raw_Validation Class. Start File name validation')
            files=[f for f in os.listdir(self.batch_directory)]
            for file in files:
                if re.match(regex, file):
                    splitAt=re.split('.csv',file)
                    splitAt=(re.split('_',splitAt[0]))
                    if len(splitAt[1]) == datestamplen:
                        if len(splitAt[2]) == timestamplen:
                            shutil.copy('Prediction_Batch_Files/'+file, 'Prediction_Raw_File_Validated/Good_Data')
                            self.log.log(file1, 'Valid File. File Name: '+str(file))
                        else:
                            shutil.copy('Prediction_Batch_Files/' + file, 'Prediction_Raw_File_Validated/Bad_Data')
                            self.log.log(file1, 'InValid File. File Name: '+str(file))
                    else:
                        shutil.copy('Prediction_Batch_Files/' + file, 'Prediction_Raw_File_Validated/Bad_Data')
                        self.log.log(file1, 'InValid File. File Name: '+str(file))
                else:
                    shutil.copy('Prediction_Batch_Files/' + file, 'Prediction_Raw_File_Validated/Bad_Data')
                    self.log.log(file1, 'InValid File. File Name: '+str(file))
            self.log.log(file2, 'File Name Validation Successfully Completed. Exit from fileNameValidation method of Predict_Raw_Validation Class')
            file1.close()
            file2.close()
        except Exception as e:
            file1 = open('Prediction_Log_Details/FileNameValidation.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1,'File Name Validation Incomplete in fileNameValidation method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            self.log.log(file2,'File Name Validation Incomplete in fileNameValidation method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            file1.close()
            file2.close()


    def colLengthValidation(self,collen):
        try:
            file1 = open('Prediction_Log_Details/ColumnLengthValidation.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file2, 'Enter in colLengthValidation method of Predict_Raw_Validation Class. Column length validation started')
            self.log.log(file1, 'Enter in colLengthValidation method of Predict_Raw_Validation Class. Column length validation started')
            for file in os.listdir('Prediction_Raw_File_Validated/Good_Data/'):
                csv=pd.read_csv('Prediction_Raw_File_Validated/Good_Data/'+file)
                if csv.shape[1]==collen:
                    pass
                else:
                    shutil.move('Prediction_Raw_File_Validated/Good_Data/'+file, 'Prediction_Raw_File_Validated/Bad_Data/')
                    self.log.log(file1,'Invalid Number of Column. FileName: '+str(file))
            self.log.log(file2, 'Column length validation Completed. Exit from colLengthValidation method of Predict_Raw_Validation Class')
            self.log.log(file1, 'Column length validation Completed. Exit from colLengthValidation method of Predict_Raw_Validation Class')
            file1.close()
            file2.close()
        except Exception as e:
            file1 = open('Prediction_Log_Details/FileColumnLengthValidation.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1,'Failed Column length validation in colLengthValidation method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            self.log.log(file2,'Failed Column length validation in colLengthValidation method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            file1.close()
            file2.close()
            raise e


    def missingWholeColumnValidation(self):
        try:
            file_1 = open('Prediction_Log_Details/MissingValueInWholeColumns.txt', 'a+')
            file_2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file_1, 'Enter in missingWholeColumnValidation method of Predict_Raw_Validation Class. Start finding entire Column contain missing value')
            self.log.log(file_2, 'Enter in missingWholeColumnValidation method of Predict_Raw_Validation Class. Start process to find any Column entirely contain missing value')

            for file in os.listdir('Prediction_Raw_File_Validated/Good_Data/'):
                csv = pd.read_csv('Prediction_Raw_File_Validated/Good_Data/' +file)
                for col in csv:
                    if (len(csv[col]) - csv[col].count()) == len(csv[col]):
                        shutil.move('Prediction_Raw_File_Validated/Good_Data/'+file,
                                    'Prediction_Raw_File_Validated/Bad_Data/')
                        self.log.log(file_1, 'File Contain Missing Value. File Name: '+str(file))
            self.log.log(file_1, 'Complete finding entire Column contain missing value. Exit from missingWholeColumnValidation method of Predict_Raw_Validation Class')
            self.log.log(file_2, 'Completed process to find any Column entirely contain missing value or not. Exit from missingWholeColumnValidation method of Predict_Raw_Validation Class')
            file_1.close()
            file_2.close()

        except Exception as e:
            file_1 = open('Prediction_Log_Details/MissingValueInWholeColumns.txt', 'a+')
            file_2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file_1, 'Error while find entire Column contain missing value in missingWholeColumnValidation method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            self.log.log(file_2, 'Error while find entire Column contain missing value in missingWholeColumnValidation method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            file_1.close()
            file_2.close()
            raise e


    def moveBadDataIntoArchiveFolder(self):
        try:
            now=datetime.now()
            date=now.date()
            time=now.strftime('%H%M%S')
            file1 = open('Prediction_Log_Details/BadDataMoveArchiveLog.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Enter in badDataArchiveFolder method of Predict_Raw_Validation Class. Start moving file from Bad folder to Archive folder.')
            self.log.log(file2, 'Enter in badDataArchiveFolder method of Predict_Raw_Validation Class. Start moving file from Bad folder to Archive folder.')

            source = 'Prediction_Raw_File_Validated/Bad_Data/'
            if os.path.isdir(source):
                temp='PredictionBadArchiveFolder'
                if not os.path.isdir(temp):
                    os.mkdir(temp)
            dest = 'PredictionBadArchiveFolder/Bad_Data_'+str(date)+"_"+str(time)
            if not os.path.isdir(dest):
                os.makedirs(dest)
            files=os.listdir(source)
            for file in files:
                if file not in os.listdir(dest):
                    shutil.move(source+file, dest)
            path = 'Prediction_Raw_File_Validated/Bad_Data/'
            if os.path.isdir(path):
                shutil.rmtree(path)
            self.log.log(file1, 'Complete moving file from Bad folder to Archive folder. Exit from badDataArchiveFolder method of Predict_Raw_Validation Class')
            self.log.log(file2, 'Complete moving file from Bad folder to Archive folder and bad data folder files also deleted. Exit from badDataArchiveFolder method of Predict_Raw_Validation Class')
            file1.close()
            file2.close()
        except Exception as e:
            file1 = open('Prediction_Log_Details/BadDataMoveArchiveLog.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an Error while move File from Bad Data folder to Archive Folder in badDataArchiveFolder method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            self.log.log(file2, 'Getting an Error while move File from Bad Data folder to Archive Folder in badDataArchiveFolder method of Predict_Raw_Validation Class. Error Msg: '+str(e))
            file1.close()
            file1.close()

    def valueFromSchema(self):
        try:
            file1 = open('Prediction_Log_Details/ValueFromSchemaLog.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Enter in valueFromSchema method of Predict_Raw_Validation Class. Start getting Details from Schema')
            self.log.log(file2, 'Enter in valueFromSchema method of Predict_Raw_Validation Class. Start getting Details from Schema')
            with open(self.schema, 'r') as f:
                dic=json.load(f)
            f.close()
            self.log.log(file1, 'Successfully read schema details')
            self.log.log(file2, 'Successfully read schema details')
            fileName=dic['SampleFileName']
            dateStamp=dic['LengthOfDateStampInFile']
            timestamp=dic['LengthOfTimeStampInFile']
            colNumber=dic['NumberofColumns']
            colName=dic['ColName']
            self.log.log(file1, 'Successfully Get Value from Schema. Exit from Enter into valueFromSchema method of Predict_Raw_Validation Class')
            self.log.log(file2, 'Successfully Get Value from Schema. Exit from Enter into valueFromSchema method of Predict_Raw_Validation Class')
            file1.close()
            file2.close()
            return fileName,dateStamp, timestamp, colNumber, colName

        except KeyError as e:
            file1 = open('Prediction_Log_Details/ValueFromSchemaLog.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while read Schema details in ValueFromSchema method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            self.log.log(file2, 'Getting an error while read Schema details in ValueFromSchema method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            file1.close()
            file2.close()
            raise e

        except ValueError as e:
            file1 = open('Prediction_Log_Details/ValueFromSchemaLog.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while read Schema details in ValueFromSchema method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            self.log.log(file2, 'Getting an error while read Schema details in ValueFromSchema method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            file1.close()
            file2.close()
            raise e

        except Exception as e:
            file1 = open('Prediction_Log_Details/ValueFromSchemaLog.txt', 'a+')
            file2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file1, 'Getting an error while read Schema details in ValueFromSchema method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            self.log.log(file2, 'Getting an error while read Schema details in ValueFromSchema method of Predict_Raw_Validation Class. Error Msg: ' + str(e))
            file1.close()
            file2.close()
            raise e

    def getRegex(self):
        regex="[FlightPrice]+['\_'']+[\d_]+[\d]+\.csv"
        return regex
