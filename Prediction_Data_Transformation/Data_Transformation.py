import os
from Application_Logger.Logger import Log
import pandas as pd


class Data_Transform:
    def __init__(self):
        self.log=Log()
        self.source='Prediction_Raw_File_Validated/Good_Data/'

    def Transformation(self):
        try:
            file_1=open('Prediction_Log_Details/Prediction_Data_Transform_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file_1, 'Enter in Transformation method of Data_Transform Class. Start Data Transformation.')
            self.log.log(file_2, 'Enter in Transformation method of Data_Transform Class. Start Data Transformation.')
            for file in os.listdir(self.source):
                csv=pd.read_csv(self.source+file)
                csv.fillna('NULL', inplace=True)
                columns = ['Airline', 'Date_of_Journey', 'Source', 'Destination', 'Route', 'Dep_Time', 'Arrival_Time',
                           'Duration', 'Total_Stops', 'Additional_Info']
                for col in columns:
                    csv[col] = csv[col].apply(lambda x: "'" + str(x) + "'")
                csv.to_csv(self.source+file, header=True, index=None)
            self.log.log(file_1, 'Complete Data Transformation. Exit from Transformation method of Data_Transform Class')
            self.log.log(file_2, 'Complete Data Transformation. Exit from Transformation method of Data_Transform Class')
            file_1.close()
            file_2.close()
        except Exception as e:
            file_1 = open('Prediction_Log_Details/Prediction_Data_Transform_Log.txt', 'a+')
            file_2 = open('Prediction_Log_Details/General_Log.txt', 'a+')
            self.log.log(file_1, 'Getting an error while perform Data Transformation. In Transformation method of Data_Transform Class. Error Msg: '+str(e))
            self.log.log(file_2, 'Getting an error while perform Data Transformation. In Transformation method of Data_Transform Class. Error Msg: '+str(e))
            file_1.close()
            file_2.close()
