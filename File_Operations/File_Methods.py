import os
import pickle
import shutil


class File_Operation:
    def __init__(self,log,file_object):
        self.log=log
        self.file_object=file_object
        self.Madel_base_path='Madel/'

    def SaveMadel(self,madel,filename):
        self.log.log(self.file_object,'Entered in Save_Madel method of the File_Operation class. To Save Madel')
        try:
            path=os.path.join(self.Madel_base_path,filename)
            if os.path.isdir(path):
                shutil.rmtree(self.Madel_base_path)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path+'/'+filename+'.sav', 'wb') as f:
                pickle.dump(madel, f)
            self.log.log(self.file_object, 'Madel '+str(filename)+' Save Successfully')
            return ' Successfully '
        except Exception as e:
            self.log.log(self.file_object,'Getting an error while Saving Madel '+str(filename)+' in SaveMadel method of File_Operation Class. Error Message: ' + str(e))
            raise e

    def LoadMadel(self,filename):
        self.log.log(self.file_object,'Entered in LoadMadel method of the File_Operation class. To load Madel for Prediction.')
        try:
            with open(self.Madel_base_path+'/'+filename+'/'+filename+'.sav', 'rb') as f:
                self.log.log(self.file_object,'Madel '+str(filename)+' loaded Successfully')
                return pickle.load(f)
        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while Loading Madel ' + str(filename) + ' in LoadMadel method of File_Operation Class. Error Message: ' + str(e))
            raise e

    def FindCorrectMadel(self,clusterno):
        self.log.log(self.file_object,"Entered in FindCorrectMadel method of the File_Operation class. To Find a Madel corrosponding the specfic Cluster")
        try:
            self.folder_name=self.Madel_base_path
            self.cluster_no=clusterno
            self.list_of_files=[]
            self.list_of_files=os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if(self.file.index(str(self.cluster_no))!=-1):
                        self.madel=self.file
                except:
                    continue
            self.madel=self.madel.split('.')[0]
            self.log.log(self.file_object,'Get the Correct Madel based on Cluster Number. Exit from FindCorrectMadel method of File_Operation Class')
            return self.madel
        except Exception as e:
            self.log.log(self.file_object, 'Getting an error while Find the correct Madel in FindCorrectMadel method of File_Operation Class. Error Message: ' + str(e))
            raise e