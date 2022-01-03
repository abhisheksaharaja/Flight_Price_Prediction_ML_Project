"""
            This is the Entry point for Prediction the Machine Learning Model.

                                     Written By: Abhishek Saha
                                     Version: 1.0
                                     Revisions: None
"""


#doing all necessary imports

import os.path
import pandas as pd
from Prediction_Raw_Data_Validation.Predict_Raw_Data_Validation import Predict_Raw_Validation
from Data_Ingestion.Prediction_Data_Loader import PredictLoadData
from Application_Logger.Logger import Log
from Data_Preprocessing.Preprocessing import Data_Preprocessing
from File_Operations.File_Methods import File_Operation


class predictionMadel:
    def __init__(self, path):
        self.log=Log()
        self.file_object=open('Prediction_Log_Details/MadelPrediction_Log.txt', 'a+')
        if path is not None:
            self.raw_data=Predict_Raw_Validation(path)


    def madelPrediction(self):
        try:

            # delete the existing prediction file generate from last run!
            if(self.raw_data.deletePredictFile()):
                self.log.log(self.file_object, 'Deleted last Predicted output file')
            else:
                self.log.log(self.file_object, 'Last Predicted output file exist')

            # Logging the start of Prediction
            self.log.log(self.file_object, 'Start Prediction')
            data_load_obj=PredictLoadData(self.log, self.file_object)

            # Getting the data from the source
            data=data_load_obj.getData()

            # Data preprocessing object Initialization
            preprocess=Data_Preprocessing(self.log, self.file_object)

            # removing all duplicate rows
            data = preprocess.RemoveDuplicateRow(data)

            # check if missing values are present in the dataset also getting response and those columns have null value
            IsNull, col_miss = preprocess.IsNullPresent(data)

            # If missing values are there, handling those missing values.
            if IsNull:
                x = preprocess.ImputeMissingValue(data, col_miss)
            else:
                x=data

            # Separate date and month of Date_of_Journey feature
            x = preprocess.DateOfJourneyFeatureFormat(x, 'Date_of_Journey')

            # Separate Hour and minute of Dep_Time feature
            x = preprocess.TimeFeatureFormat(x, 'Dep_Time')

            # Separate Hour and minute of Arrival_Time feature
            x = preprocess.TimeFeatureFormat(x, 'Arrival_Time')

            # Separate Hour and minute of Duration feature
            x = preprocess.TimeFeatureFormat_1(x, 'Duration')

            # Perform Label Encoding on Airline, Source, Destination, Total_Stops Features
            x = preprocess.LabelEncoding(x)

            lt = ['Date_of_Journey', 'Route', 'Dep_Time', 'Arrival_Time', 'Duration', 'Additional_Info']

            # Drop some Columns
            x = preprocess.ColumnDrop(x, lt)

            file_loader = File_Operation(self.log, self.file_object)

            # Load the Created Kmean Cluster Madel
            kmeans=file_loader.LoadMadel('kmeans')

            # Predict Number of cluster for Prediction dataset by Cluster Madel
            cluster=kmeans.predict(x)
            x['cluster']=cluster

            # Getting the unique clusters from our dataset
            lst_cluster=x['cluster'].unique()

            #Check output directory present or not if alredy created ignore the block of code or create the directory
            if not os.path.isdir('Predicted_Output_File'):
                os.mkdir('Predicted_Output_File')

            # Parsing all the clusters and looking for the Existing ML Madel to predict the outcome and store result in CSV format in Predicted_Output_File Directory
            for i in lst_cluster:
                # filter the data for each cluster
                cluster_data=x[x['cluster']==i]

                #Drop the Cluster Feature from dataset
                cluster_data=cluster_data.drop(['cluster'], axis=1)

                # Try to find the correct Madel
                madel_name=file_loader.FindCorrectMadel(i)

                # Load the Madel corresponding to Cluster Number
                madel=file_loader.LoadMadel(madel_name)

                # Predict the result
                result=list(madel.predict(cluster_data))

                # Create a Dataframe
                result=pd.DataFrame(result, columns=['Predicted_Flight_Price'])
                path='Predicted_Output_File/Prediction.csv'

                # Store the predicted outcome into CSV format
                result.to_csv('Predicted_Output_File/Prediction.csv', header=True, mode='a+')
            self.log.log(self.file_object, 'End of Prediction. Exit from madelPrediction method of PredictionMadel Class')
            return path, result.head().to_json(orient="records")

        except Exception as e:

            # Logging the unsuccessful Prediction
            self.log.log(self.file_object, 'Error occurred while running the prediction!! Error Msg:'+str(e))
            self.file_object.close()
            raise e



