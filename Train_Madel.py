from Data_Ingestion.Data_Loader import LoadData
from Application_Logger.Logger import Log
from Data_Preprocessing.Preprocessing import Data_Preprocessing
from Data_Preprocessing.Clustering import KMeansClustering
from sklearn.model_selection import train_test_split
from Madel_Finder.Tuner import MadelTuner
from File_Operations.File_Methods import File_Operation


class TrainingMadel:
    def __init__(self):
        self.log=Log()
        self.file_object=open('Training_Log_Details/MadelTrainingLog.txt', 'a+')

    def MadelTraining(self):
        try:
            self.log.log(self.file_object, 'Start Madel Training')

            data_load_obj=LoadData(self.log, self.file_object)
            data=data_load_obj.GetData()

            Preprocess=Data_Preprocessing(self.log,self.file_object)
            data=Preprocess.RemoveDuplicateRow(data)

            x,y=Preprocess.SeperateLabelFeature(data,'Price')

            IsNull, col_miss= Preprocess.IsNullPresent(x)

            if(IsNull):
                x=Preprocess.ImputeMissingValue(x, col_miss)

            x=Preprocess.DateOfJourneyFeatureFormat(x,'Date_of_Journey')

            x=Preprocess.TimeFeatureFormat(x,'Dep_Time')

            x=Preprocess.TimeFeatureFormat(x,'Arrival_Time')

            x=Preprocess.TimeFeatureFormat_1(x,'Duration')

            x=Preprocess.LabelEncoding(x)

            lt=['Date_of_Journey','Route','Dep_Time','Arrival_Time','Duration','Additional_Info']
            x=Preprocess.ColumnDrop(x,lt)


            Kmeans_obj=KMeansClustering(self.log,self.file_object)

            no_cluster=Kmeans_obj.FindClusterNumber(x)

            x=Kmeans_obj.CreateCluster(x,no_cluster)

            # create a new column in the dataset consisting of the corresponding cluster assignments.

            x['Labels']=y

            lst_cluster=x['Cluster'].unique()
            self.log.log(self.file_object, 'Start to Find Madel')
            for i in lst_cluster:
                cluster_data=x[x['Cluster']==i]
                cluster_feature=cluster_data.drop(['Cluster','Labels'],axis=1)
                cluster_label=cluster_data['Labels']

                x_train,x_test,y_train,y_test=train_test_split(cluster_feature,cluster_label, test_size=0.25)
                madel_finder=MadelTuner(self.log,self.file_object)

                best_madel_name, best_madel=madel_finder.find_best_madel(x_train,y_train,x_test,y_test)

                fp=File_Operation(self.log, self.file_object)
                msg=fp.SaveMadel(best_madel, best_madel_name+str(i))

                self.log.log(self.file_object, 'Best Madel : '+str(best_madel_name)+' saved '+str(msg))
            self.log.log(self.file_object, 'Successfully saved all Madel for each Cluster.')
            self.log.log(self.file_object,'End Successful Madel Training. Exit from MadelTraining method of TrainingMadel Class')
            self.file_object.close()

        except Exception as e:
            self.log.log(self.file_object, 'Unsuccessful Madel Training. Error Msg: '+str(e))
            self.file_object.close()
            raise e








