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
            if(self.raw_data.deletePredictFile()):
                self.log.log(self.file_object, 'Deleted last Predicted output file')
            else:
                self.log.log(self.file_object, 'Last Predicted output file exist')

            self.log.log(self.file_object, 'Start Prediction')
            data_load_obj=PredictLoadData(self.log, self.file_object)
            data=data_load_obj.getData()

            preprocess=Data_Preprocessing(self.log, self.file_object)
            data = preprocess.RemoveDuplicateRow(data)

            IsNull, col_miss = preprocess.IsNullPresent(data)

            if IsNull:
                x = preprocess.ImputeMissingValue(data, col_miss)
            else:
                x=data
            x = preprocess.DateOfJourneyFeatureFormat(x, 'Date_of_Journey')

            x = preprocess.TimeFeatureFormat(x, 'Dep_Time')

            x = preprocess.TimeFeatureFormat(x, 'Arrival_Time')

            x = preprocess.TimeFeatureFormat_1(x, 'Duration')

            x = preprocess.LabelEncoding(x)

            lt = ['Date_of_Journey', 'Route', 'Dep_Time', 'Arrival_Time', 'Duration', 'Additional_Info']
            x = preprocess.ColumnDrop(x, lt)

            file_loader = File_Operation(self.log, self.file_object)
            kmeans=file_loader.LoadMadel('kmeans')

            cluster=kmeans.predict(x)
            x['cluster']=cluster

            lst_cluster=x['cluster'].unique()

            if not os.path.isdir('Predicted_Output_File'):
                os.mkdir('Predicted_Output_File')

            for i in lst_cluster:
                cluster_data=x[x['cluster']==i]
                cluster_data=cluster_data.drop(['cluster'], axis=1)
                madel_name=file_loader.FindCorrectMadel(i)
                madel=file_loader.LoadMadel(madel_name)
                result=list(madel.predict(cluster_data))
                result=pd.DataFrame(result, columns=['Predicted_Flight_Price'])
                path='Predicted_Output_File/Prediction.csv'
                result.to_csv('Predicted_Output_File/Prediction.csv', header=True, mode='a+')
            self.log.log(self.file_object, 'End of Prediction. Exit from madelPrediction method of PredictionMadel Class')
            return path, result.head().to_json(orient="records")

        except Exception as ex:
            self.log.log(self.file_object, 'Error occurred while running the prediction!! Error Msg:'+str(e))
            self.file_object.close()
            raise ex



