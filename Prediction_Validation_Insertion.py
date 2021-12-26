from Prediction_Raw_Data_Validation.Predict_Raw_Data_Validation import Predict_Raw_Validation
from Application_Logger.Logger import Log
from Prediction_Data_Transformation.Data_Transformation import Data_Transform
from Prediction_DB_Operation.Prediction_Data_DB_Operation import DBOperation

class Predict_Validation:
    def __init__(self,path):
        self.raw_data=Predict_Raw_Validation(path)
        self.log=Log()
        self.file_object=open('Prediction_Log_Details/Prediction_Main_Log.txt', 'a+')
        self.data_transform=Data_Transform()
        self.dboperation=DBOperation()

    def Prediction(self):
        try:
            self.log.log(self.file_object, 'Start Raw Data Validation For Prediction Dataset')
            fileName,dateStamp, timestamp, colNumber, colName=self.raw_data.valueFromSchema()
            regex=self.raw_data.getRegex()
            self.raw_data.fileNameValidation(regex,dateStamp,timestamp)
            self.raw_data.colLengthValidation(colNumber)
            self.raw_data.missingWholeColumnValidation()
            self.log.log(self.file_object, 'Raw Data Validation Completed')

            self.log.log(self.file_object, 'Start Data Transformation')
            self.data_transform.Transformation()
            self.log.log(self.file_object, 'Completed Data Transformation')

            self.log.log(self.file_object, 'Database Operation Started')
            self.dboperation.createTable('Prediction', colName)
            self.log.log(self.file_object, 'Table Created')

            self.log.log(self.file_object, 'Data insertion into Table')
            self.dboperation.insertIntodb('Prediction')
            self.log.log(self.file_object, 'Data Successfully insert into Table')

            self.log.log(self.file_object, 'Deleting Existing Good Data Folder')
            self.raw_data.deleteExistingGoodDataFolder()
            self.log.log(self.file_object, 'Successfully Deleted Existing Good Data Folder')

            self.log.log(self.file_object, 'Start moving Bad Data Folder into Bad Data Archive Folder')
            self.raw_data.moveBadDataIntoArchiveFolder()
            self.log.log(self.file_object, 'Successfully move Bad Data Folder into Bad Data Archive Folder')

            self.log.log(self.file_object, 'Start moving Data from Database Table to CSV')
            self.dboperation.tabletoCSV('Prediction')
            self.log.log(self.file_object, 'Successfully load data from Table to CSV')
            self.log.log(self.file_object, 'Database Operation Ended')
            self.log.log(self.file_object, 'End Raw Data Validation For Prediction Dataset. Exit from Prediction method of Predict_Validation Class')
            self.file_object.close()

        except Exception as e:
            self.logger.log(self.file_object, 'Getting an error while Perform Raw Data Validation For Prediction Dataset. Error Msg: '+str(e))
            self.file_object.close()
            raise e







