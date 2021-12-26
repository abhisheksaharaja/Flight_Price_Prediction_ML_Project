from datetime import datetime

class Log:
    def __init__(self):
        pass

    def log(self,file_object,msg):
        self.now=datetime.now()
        self.date=self.now.date()
        self.time=self.now.strftime('%H:%M:%S')
        file_object.write(str(self.date)+" / "+str(self.time)+"\t"+msg+"\n")