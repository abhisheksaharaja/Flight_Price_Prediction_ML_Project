#doing all necessary import

from Application_Logger.Logger import Log
import os
import pandas as pd

class Data_Transform:

    """
                     This class shall be used for transforming the Good Raw Training Data before loading
                     it in Database!!.
    """

    def __init__(self):
        self.log=Log()
        self.file_path='Training_Raw_File_Validated/Good_Data/'

    def Transformation(self):

        """
                                      Method Name: data_transformation
                                      Description: This method replaces the missing values in columns with
                                                   "NULL" to store in the table and typecast all data into
                                                    string to store into Database.
                                      Output: None
                                      On Failure: Raise Exception
        """

        try:
            f=open('Training_Log_Details/Data_Transformation_Log.txt','a+')
            self.log.log(f,'Enter in Transformation method of Data_Transform Class. Data Transformation Started')
            for file in os.listdir(self.file_path):
                csv = pd.read_csv(self.file_path + file)
                csv.fillna('NULL', inplace=True)
                columns = ['Airline', 'Date_of_Journey', 'Source', 'Destination', 'Route', 'Dep_Time', 'Arrival_Time',
                           'Duration', 'Total_Stops', 'Additional_Info']
                for col in columns:
                    csv[col] = csv[col].apply(lambda x: "'" + str(x) + "'")
                csv.to_csv(self.file_path + file, header=True, index=None)
            self.log.log(f, 'Completed Data Transformation. Exit from Transformation method of Data_Transform Class.')
            f.close()
        except Exception as e:
            f = open('Training_Log_Details/Data_Transformation_Log.txt', 'a+')
            self.log.log(f, 'Getting an error while Perform Data Transformation in Transformation method of Data_Transform Class. Error Msg: '+str(e))
            f.close()
            raise e