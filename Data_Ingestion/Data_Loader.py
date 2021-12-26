import pandas as pd

class LoadData:
    def __init__(self,logger,file_object):
        self.log=logger
        self.file_object=file_object
        self.data_path='Training_File_From_DB/Input.csv'

    def GetData(self):
        try:
            self.log.log(self.file_object,'Enter in GetData method of Load Data Class.')
            self.data=pd.read_csv(self.data_path)
            self.log.log(self.file_object, 'Data read Successfully!!! Exit from GetData method of Load Data Class.')
            return self.data

        except Exception as e:
            self.log.log(self.file_object, 'Getting an Error while reading data from GetData method of LoadData Class'+str(e))
            raise e