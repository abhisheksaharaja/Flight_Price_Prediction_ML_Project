from Application_Logger.Logger import Log
from Training_Raw_Data_Validation.Raw_Data_Validation import Raw_Validation
from Training_Data_Transformation.Data_Transformation import Data_Transform
from Training_DB_Opeartion.Training_Data_DB_Operation import DBOperation


class Training_Validation:
    def __init__(self,path):
        self.raw_data=Raw_Validation(path)
        self.logger=Log()
        self.file_object=open('Training_Log_Details/Training_Main_Log.txt', 'a+')
        self.data_transform=Data_Transform()
        self.DBOperation=DBOperation()

    def Training(self):
        try:
            self.logger.log(self.file_object,' Start Raw Data Validation for Madel Training Dataset.')
            datestamp, timestamp, numcol, colname = self.raw_data.ValueFromSchema()
            regex=self.raw_data.mutual_regex()
            self.raw_data.FileNameValidation(regex,datestamp, timestamp)
            self.raw_data.ColumnLengthValidation(numcol)
            self.raw_data.MissingValueInWholeColumnValidation()
            self.logger.log(self.file_object, 'Raw Data Validation Completed')

            self.logger.log(self.file_object, 'Start Data Transformation')
            self.data_transform.Transformation()
            self.logger.log(self.file_object, 'End with Data Transformation Process')

            self.logger.log(self.file_object,'Database Operation Started')
            self.DBOperation.createTable('Training',colname)
            self.logger.log(self.file_object, 'Table Created')

            self.logger.log(self.file_object, 'Data insertion into Table')
            self.DBOperation.InsertIntoDB('Training')
            self.logger.log(self.file_object, 'Data Successfully insert into Table')

            self.logger.log(self.file_object, 'Deleteing Existing Good Data Folder')
            self.raw_data.DeletExistingGoodData()
            self.logger.log(self.file_object, 'Successfully Deleted Existing Good Data Folder')

            self.logger.log(self.file_object, 'Start moving Bad Data Folder into Bad Data Archive Folder')
            self.raw_data.MoveBadArchiveFolder()
            self.logger.log(self.file_object, 'Successfully move Bad Data Folder into Bad Data Archive Folder')

            self.logger.log(self.file_object, 'Start moving Data from Database Table to CSV')
            self.DBOperation.InsertIntoCSV('Training')
            self.logger.log(self.file_object, 'Successfully load data from Table to CSV')
            self.logger.log(self.file_object, 'Database Operation Ended')
            self.logger.log(self.file_object, 'Ended Raw Data Validation for Madel Training Dataset. Exit from Training method of Training_Validation Class')
            self.file_object.close()

        except Exception as e:
            self.logger.log(self.file_object, 'Getting an error while Perform Raw Data Validation for Madel Training Dataset. Error Msg: '+str(e))
            self.file_object.close()
            raise e








