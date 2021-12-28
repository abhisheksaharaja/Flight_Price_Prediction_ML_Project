"""
            This is the Entry point for Training the Machine Learning Model.
"""
# Doing the necessary imports

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
            # Logging the start of Training
            self.log.log(self.file_object, 'Start Madel Training')

            # Getting the data from the source
            data_load_obj=LoadData(self.log, self.file_object)
            data=data_load_obj.GetData()

            # doing the data preprocessing
            Preprocess=Data_Preprocessing(self.log,self.file_object)

            # remove all duplicate rows
            data=Preprocess.RemoveDuplicateRow(data)

            # Separate features and label
            x,y=Preprocess.SeperateLabelFeature(data,'Price')

            # check if missing values are present in the dataset also getting response and those columns have nullvalue
            IsNull, col_miss= Preprocess.IsNullPresent(x)

            # if missing values are there, handling those missing values.
            if(IsNull):
                x=Preprocess.ImputeMissingValue(x, col_miss)# missing value imputation

            # Separate date and month of Date_of_Journey feature
            x=Preprocess.DateOfJourneyFeatureFormat(x,'Date_of_Journey')

            # Separate Hour and minute of Dep_Time feature
            x=Preprocess.TimeFeatureFormat(x,'Dep_Time')

            # Separate Hour and minute of Arrival_Time feature
            x=Preprocess.TimeFeatureFormat(x,'Arrival_Time')

            # Separate Hour and minute of Duration feature
            x=Preprocess.TimeFeatureFormat_1(x,'Duration')

            # Perform Label Encoding on Airline, Source, Destination, Total_Stops Features
            x=Preprocess.LabelEncoding(x)

            lt=['Date_of_Journey','Route','Dep_Time','Arrival_Time','Duration','Additional_Info']
            # Drop some Columns
            x=Preprocess.ColumnDrop(x,lt)


            # Applying the clustering approach
            Kmeans_obj=KMeansClustering(self.log,self.file_object) # object initialization.

            # using the elbow plot to find the number of optimum clusters
            no_cluster=Kmeans_obj.FindClusterNumber(x)

            # Divide the data into clusters
            x=Kmeans_obj.CreateCluster(x,no_cluster)

            # create a new column in the dataset consist the label feature
            x['Labels']=y

            # getting the unique clusters from our dataset
            lst_cluster=x['Cluster'].unique()

            # parsing all the clusters and looking for the best ML algorithm to fit on individual cluster
            self.log.log(self.file_object, 'Start to Find Madel')
            for i in lst_cluster:
                cluster_data=x[x['Cluster']==i] # filter the data for each cluster

                # Prepare the feature and Label columns
                cluster_feature=cluster_data.drop(['Cluster','Labels'],axis=1)
                cluster_label=cluster_data['Labels']

                # splitting the data into training and test set for each cluster one by one
                x_train,x_test,y_train,y_test=train_test_split(cluster_feature,cluster_label, test_size=0.25)

                # object initialization
                madel_finder=MadelTuner(self.log,self.file_object)

                # getting the best model for each of the clusters
                best_madel_name, best_madel=madel_finder.find_best_madel(x_train,y_train,x_test,y_test)

                # saving the best model to the directory.
                fp=File_Operation(self.log, self.file_object)
                msg=fp.SaveMadel(best_madel, best_madel_name+str(i))

                self.log.log(self.file_object, 'Best Madel : '+str(best_madel_name)+' saved '+str(msg))

            # logging the successful Training
            self.log.log(self.file_object, 'Successfully saved all Madel for each Cluster.')
            self.log.log(self.file_object,'End Successful Madel Training. Exit from MadelTraining method of TrainingMadel Class')
            self.file_object.close()

        except Exception as e:

            # logging the unsuccessful Training
            self.log.log(self.file_object, 'Unsuccessful Madel Training. Error Msg: '+str(e))
            self.file_object.close()
            raise e








