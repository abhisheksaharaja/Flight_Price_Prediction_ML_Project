import pandas as pd

class PredictLoadData:

    """
            This class shall  be used for obtaining the data from the source for prediction.

                                     Written By: Abhishek Saha
                                     Version: 1.0
                                     Revisions: None
    """

    def __init__(self,log,file_object):
        self.log=log
        self.file_object=file_object
        self.data_path='Prediction_File_From_DB/InputFile.csv'

    def getData(self):

        """
                                        Method Name: getData
                                        Description: This method reads the data from source for Prediction.
                                        Output: A pandas DataFrame.
                                        On Failure: Raise Exception


                                        Written By: Abhishek Saha
                                        Version: 1.0
                                        Revisions: None
        """

        try:
            self.log.log(self.file_object, 'Enter in GetData method of PredictLoadData Class.')
            self.data=pd.read_csv(self.data_path)
            self.log.log(self.file_object, 'Data Loaded Successfully. Exit from GetData method of PredictLoadData Class.')
            return self.data

        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while Loading Data. In getData method of PredictLoadData Class. Error Msg: '+str(e))
            raise e




