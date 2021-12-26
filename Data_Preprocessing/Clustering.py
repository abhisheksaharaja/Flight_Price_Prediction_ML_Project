import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from File_Operations.File_Methods import File_Operation


class KMeansClustering:
    def __init__(self,log, file_object):
        self.log=log
        self.file_object=file_object

    def FindClusterNumber(self,x):
        self.log.log(self.file_object,'Enter in PlotElbow method of KMeansClustering method. To Find Cluster number.')
        try:
            wcss=[]
            for i in range(1,15):
                Kmeans=KMeans(n_clusters=i,init='k-means++')
                Kmeans.fit(x)
                wcss.append(Kmeans.inertia_)

            # plt.plot(range(1, 15), wcss)
            # plt.title('Elbow Graph')
            # # plt.xlabel('Number of clusters')
            # plt.ylabel('WCSS')
            # plt.savefig('Preprocessing_Training_Data/Elbow.PNG')

            self.kn = KneeLocator(range(1,15), wcss, curve='concave', direction='decreasing')
            self.log.log(self.file_object,'Optimum minimum Cluster: '+str(self.kn.knee))
            self.log.log(self.file_object, 'Successfully find the cluster number. Exit from FindClusterNumber method of KMeansClustering Class.')
            return self.kn.knee
        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while find Cluster number in FindClusterNumber method of KMeansClustering Class. Error Message: '+str(e))
            raise e

    def CreateCluster(self,x,cluster_number):
        self.data = x
        self.log.log(self.file_object,'Enter in CreateCluster method of KMeansClustering Class. To create CreateCluster.')
        try:
            self.kmeans=KMeans(n_clusters=cluster_number)
            self.kmeans_cluster=self.kmeans.fit_predict(x)

            file_operation_obj=File_Operation(self.log, self.file_object)
            msg=file_operation_obj.SaveMadel(self.kmeans,'kmeans')

            self.data['Cluster']=self.kmeans_cluster
            self.log.log(self.file_object, 'Successfully Created '+str(cluster_number)+' number of Clsuer and Save madel'+str(msg)+'for CreateCluster method of KMeansClustering Class.')
            self.log.log(self.file_object, 'Exit from CreateCluster method of KMeansClustering Class.')
            return self.data
        except Exception as e:
            self.log.log(self.file_object,'Getting an error while find Cluster number in CreateCluster method of KMeansClustering Class. Error Message: ' + str(e))
            raise e