#doing all necessary imports

import math
import pandas as pd
import numpy as num
from sklearn.impute import KNNImputer
import matplotlib.pyplot as plt
import seaborn as sns


class Data_Preprocessing:

    """
                    This class shall be used to clean and transform the data before training.

    """

    def __init__(self,log,file_object):
        self.log=log
        self.file_object=file_object

    def RemoveDuplicateRow(self,data):

        """
                                     Method Name: RemoveDuplicateRow
                                     Description: This method remove duplicate rows from a pandas dataframe .
                                     Output: A pandas DataFrame contain unique rows.
                                     On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in RemoveDuplicateRow method of Data_Preprocessing Class. Start removing Duplicate Rows')
        self.data=data
        try:
            temp=self.data.duplicated().sum()
            if temp>0:
                self.log.log(self.file_object, 'Duplicate Rows: '+str(temp))
                self.data.drop_duplicates(inplace=True)
            self.log.log(self.file_object, 'Remove Duplicate Rows. File Contain only unique Rows. Exit from RemoveDuplicateRow method of Data_Preprocessing Class')
            return self.data

        except Exception as e:
            self.log.log(self.file_object, 'Getting some error while Removing duplicate rows in RemoveDuplicateRow method of Data_Preprocessing Class'+str(e))
            raise Exception

    def SeperateLabelFeature(self,data,col):

        """
                                        Method Name: SeperateLabelFeature
                                        Description: This method separates the features and a Label Columns.
                                        Output: Returns two separate Dataframes, one containing features and
                                                the other containing Labels .
                                        On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in SeperateLabelFeature method of Data_Preprocessing Class .Starting Separate Label Feature')
        try:
            self.x=data.drop(labels=col,axis=1)
            self.y=data[col]
            self.log.log(self.file_object,'Label Feature Separation Completed. Exit from SeperateLabelFeature method of Data_Preprocessing Class')
            return self.x,self.y

        except Exception as e:
            self.log.log(self.file_object,'Label Feature Seperation unsuccessful, Error Message: '+str(e))
            raise e

    def IsNullPresent(self,x):

        """
                                       Method Name: IsNullPresent
                                       Description: This method checks whether there are null values present
                                                    in the pandas Dataframe or not.
                                       Output: Returns a Boolean Value and columns contain null value.True if
                                                null values are present in the DataFrame, False if they are not present.
                                       On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in IsNullPresent method of Data_Preprocessing Class. Start Count Number of Null Values')
        self.Null_Present=False
        self.cols_with_missing_values=[]
        self.cols = x.columns
        try:
            self.null_count=x.isna().sum()
            for i in range(len(self.null_count)):
                if self.null_count[i] > 0:
                    self.Null_Present = True
                    self.cols_with_missing_values.append(self.cols[i])
            if (self.Null_Present):
                self.Null_CSV=pd.DataFrame()
                self.Null_CSV['Column']=x.columns
                self.Null_CSV['Count']=num.asarray(x.isna().sum())
                self.Null_CSV.to_csv('Preprocessing_Training_Data/Null_Count.csv')
            self.log.log(self.file_object, 'Finding missing values is a success.Data written to the null values file. Exit from IsNullPresent method of Data_Preprocessing Class')
            return self.Null_Present, self.cols_with_missing_values

        except Exception as e:
            self.log.log(self.file_object,'Exception occured in IsNullPresent method of the Preprocessor class. Error Message: '+str(e))
            raise e

    def ImputeMissingValue(self,data,col):

        """
                                      Method Name: ImputeMissingValue
                                      Description: This method replaces all the missing values in the Dataframe
                                                   By Random sample Imputation Technique .
                                      Output: A Dataframe which has all the missing values imputed.
                                      On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in ImputeMissingValue method of Data_Preprocessing Class. Start Imputing missing Values')
        self.data=data
        try:
            # knn_imputer_obj=KNNImputer(n_neighbors=3,weights='uniform',missing_values=num.nan)
            # self.temp=knn_imputer_obj.fit_transform(self.data)
            # self.new_data=pd.DataFrame(data=self.temp,columns=self.data.columns)
            # self.log.log(self.file_object, 'Sucsessfully impute missing value')

            for variable in col:
                # self.data[variable] = self.data[variable]
                sample = self.data[variable].dropna().sample(self.data[variable].isna().sum(), random_state=0)
                sample.index = self.data[self.data[variable].isna()].index
                self.data.loc[self.data[variable].isna(), variable] = sample

            self.log.log(self.file_object, 'End of Imputing missing Values. Exit from ImputeMissingValue method of Data_Preprocessing Class')

            return self.data

        except Exception as e:
            self.log.log(self.file_object, 'Error Occoured while Impute the Missing value In ImputeMissingValue method of Data_Preprocessing Class. Error Message: '+str(e))
            raise e

    def DateOfJourneyFeatureFormat(self,data,colname):

        """
                                          Method Name: DateOfJourneyFeatureFormat
                                          Description: This method Formatting the Date_of_Journey feature.
                                          Output: A pandas DataFrame after Formatting the Date_of_Journey feature.
                                          On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in DateOfJourneyFeatureFormat method of Data_Preprocessing Class .Start Formatting Date of Journey Feature')
        self.data = data
        try:
            # self.data[colname+'_Day']=pd.to_datetime(data[colname] , format='%d/%m/%y').dt.day
            # self.data[colname+'_Month'] = pd.to_datetime(self.data[colname], format='%d/%m/%y').dt.month
            self.data[colname]=pd.to_datetime(self.data[colname])
            self.data[colname+'_Day']=self.data[colname].dt.day
            self.data[colname + '_Month']=self.data[colname].dt.month

            self.log.log(self.file_object, 'Successfully impute Date of Journey Feature also create two new feature to store day and month seperately. Exit from DateOfJourneyFeatureFormat method of Data_Preprocessing Class')
            return self.data

        except Exception as e:
            self.log.log(self.file_object,'Error Occurred while Impute the Date of Journey Feature In DateOfJourneyFeatureFormat method of Data_Preprocessing Class. Error Message: ' + str(e))
            raise e

    def TimeFeatureFormat(self,data,col):

        """
                                         Method Name: TimeFeatureFormat
                                         Description: This method Formatting the Dep_Time, Arrival_Time feature.
                                         Output: A pandas DataFrame after Formatting the Dep_Time, Arrival_Time feature.
                                         On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in TimeFeatureFormat method of Data_Preprocessing Class. Start Formatting Time Feature')
        self.data=data
        try:
            self.data[col + '_Hour'] = pd.to_datetime(self.data[col]).dt.hour
            self.data[col + '_Minute'] = pd.to_datetime(self.data[col]).dt.minute
            self.log.log(self.file_object,'Sucsessfully impute Time Format Feature also create two new feature to store Hour and Minute seperately. Exit from TimeFeatureFormat method of Data_Preprocessing Class.')
            return self.data

        except Exception as e:
            self.log.log(self.file_object,'Error Occoured while Impuet the TimeFeatureFormat method of Data_Preprocessing Class. Error Message: ' + str(e))
            raise e

    def TimeFeatureFormat_1(self,data,colname):

        """
                                        Method Name: TimeFeatureFormat_1
                                        Description: This method Formatting the Duration feature.
                                        Output: A pandas DataFrame after Formatting the Duration feature.
                                        On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Enter in TimeFeatureFormat_1 method of Data_Preprocessing Class. Start Duration Feature Formatting')
        try:
            temp = list(data[colname])
            for i in range(len(temp)):
                if 'h' not in temp[i]:
                    temp[i] = '0h ' + temp[i]
                if 'm' not in temp[i]:
                    temp[i] = temp[i].strip() + ' 0m'

            duration_hour = []
            duration_min = []

            for i in range(len(temp)):
                duration_hour.append(temp[i].split(sep='h')[0])
                duration_min.append(temp[i].split(sep='m')[0].split()[-1])

            data['duration_hour'] = duration_hour
            data['duration_min'] = duration_min

            data['duration_hour'] = data['duration_hour'].astype(int)
            data['duration_min'] = data['duration_min'].astype(int)
            self.log.log(self.file_object, 'Successfully Completed Duration Feature Formatting. Exit from TimeFeatureFormat_1 method of Data_Preprocessing Class.')
            return data

        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while PErform Duration Feature Formatting in TimeFeatureFormat_1 method of Data_Preprocessing Class. Error Message: '+str(e))
            raise e

    def LabelEncoding(self,x):

        """
                                         Method Name: LabelEncoding
                                         Description: This method encode Airline, destination, source and
                                                      total_stops feature.
                                         Output: A pandas DataFrame after encoding Airline, destination, source
                                                 and total_stops feature.
                                         On Failure: Raise Exception
        """

        self.log.log(self.file_object,'Enter in LabelEncoding method of Data_Preprocessing Class. Label Encoding Started')
        try:
            sns.countplot(y='Airline', data=x)
            plt.ylabel('Airline')
            plt.savefig('Preprocessing_Training_Data/Airline_Count_Plot.PNG')
            # plt.title('')
            Label_1={
                "Trujet": 12, "Jet Airways": 11, "IndiGo": 10, "Air India": 9, "Multiple carriers": 8,
                "SpiceJet": 7, "Vistara": 6, "Air Asia": 5, "GoAir": 4,
                "Multiple carriers Premium economy": 3, "Jet Airways Business": 2,
                "Vistara Premium economy": 1
            }
            x['Airline'] = x['Airline'].map(Label_1)
            self.log.log(self.file_object, 'Airline Encoding done Successfully')

            sns.countplot(y='Source', data=x)
            plt.ylabel('Source')
            plt.savefig('Preprocessing_Training_Data/Source_Count_Plot.PNG')
            # plt.title('')
            Label_2 = {
                "Chennai": 1,"Mumbai": 2, "Banglore": 3, "Kolkata": 4, "Delhi": 5
            }
            x['Source'] = x['Source'].map(Label_2)
            self.log.log(self.file_object, 'Source Encoding done Successfully')

            sns.countplot(y='Destination', data=x)
            plt.ylabel('Destination')
            plt.savefig('Preprocessing_Training_Data/Destination_Count_Plot.PNG')
            # plt.title('')
            Label_3= {
                "Kolkata": 1, "Hyderabad": 2, "New Delhi": 3, "Delhi": 4, "Banglore": 5, "Cochin": 6
             }
            x['Destination'] = x['Destination'].map(Label_3)
            self.log.log(self.file_object, 'Destination Encoding done Successfully')


            # explode = (0, 0, 0.2, 0.4, 0.3)
            # labels = ['non-stop', '2 stops', '1 stop', '3 stops', '4 stops']
            # plt.pie(x['Total_Stops'].value_counts(), autopct='%1.1f%%', shadow=True, explode=explode,
            #         labels=labels)
            sns.countplot(y='Total_Stops', data=x)
            plt.ylabel('Total_Stops')
            plt.savefig('Preprocessing_Training_Data/Total_Stops_Count_Plot.PNG')
            Label_4 = {
                "non-stop":5, "2 stops":3, "1 stop":4, "3 stops":2, "4 stops":1
            }
            x['Total_Stops'] = x['Total_Stops'].map(Label_4)

            self.log.log(self.file_object, 'Total_Stops Encoding done Successfully')
            self.log.log(self.file_object, 'Encoding Completed. Exit from LabelEncoding method of Data_Preprocessing Class.')
            return x

        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while Perform Encoding Feature  in LabelEncoding method of Data_Preprocessing Class. Error Message: '+str(e))
            raise e

    def ColumnDrop(self,data,lt):

        """
                                     Method Name: ColumnDrop
                                     Description: This method remove the columns from a pandas dataframe depend
                                                  on the column pass as a parameter.
                                     Output: A pandas DataFrame after removing the specified columns.
                                     On Failure: Raise Exception
        """

        self.log.log(self.file_object,'Enter in ColumnDrop method of Data_Preprocessing Class. Start Dropping Column')
        self.data=data
        try:
            for col in lt:
                self.data.drop(labels=col, axis=1, inplace=True)
                self.log.log(self.file_object,'Drop Column: '+str(col))
            self.log.log(self.file_object,'Successfully Drop column')
            return self.data

        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while drop Feature in ColumnDrop method of Data_Preprocessing Class. Error Message: '+str(e))
            raise e

