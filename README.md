# Ineuron-internship
Machine Learning algorithms are utilized for Flight Price  prediction.

## Table of Content 
- [Description](#Description)
- [Dataset](#Dataset)
- [Data Validation](#Data-Validation)
- [Data Insertion in Database](#Data-Insertion-in-Database)
- [Model Training](#Model-Training)
- [Setup](#Setup)
- [Create a Account at Circle ci](#create-a-account-at-circle-ci)
- [setup your project in Circle CI](#setup-your-project)
- [Environment variable setup in Circle CI](#Environment-variable-setup-in-Circle-CI)

- [Inference demo](#Inference-demo)




## Problem Statement:

Travelling through flights has become an integral part of today’s lifestyle as more and 
more people are opting for faster travelling options. The flight ticket prices increase or 
decrease every now and then depending on various factors like timing of the flights, 
destination, and duration of flights various occasions such as vacations or festive 
season. Therefore, having some basic idea of the flight fares before planning the trip will 
surely help many people save money and time.

With the help of this project we can predict the fares of the flights based on different factors available in 
the provided dataset.


## Description
    
- The client will send data in multiple sets of files in batches at a given location. 
- Data will contain Flight details from Departure to Arrival with 11 number of column for training and 10 number of column for Predicting dataset.

- Apart from training and prediction files, we also require two "schema" file from the client, which contain all the
    relevant information about the training and prediction files such as:
    Name of the files, Length of Date value in FileName, Length of Time value in FileName, NUmber of Columnns, 
    Name of Columns, and their dataype.
    
    
## Dataset
- Download the dataset for training and prediction
<a href="https://drive.google.com/drive/folders/1buLRwFO2UAT9IuCpN3RQP6RNH343jPeE?usp=sharing"> Dataset </a>
      
      

## Data Validation:
    
In This step, we perform different sets of validation on the given set of training files.

- Name Validation:
         We validate the name of the files based on the given name in the schema file. We have 
         created a regex patterg as per the name given in the schema fileto use for validation.After validating 
         the pattern in the name, we check for the length of the date in the file name as well as the length of time 
         in the file name. If all the values are as per requirements, we move such files to "Good_Data_Folder" else
         we move such files to "Bad_Data_Folder."

- Number of Columns: 
         We validate the number of columns present in the files, and if it doesn't match with the
         value given in the schema file, then the file id moves to "Bad_Data_Folder" or move into "Good_Data_Folder"

- Null values in columns: 
         If any of the columns in a file have all the values as NULL or missing, we discard such
         a file and move it to "Bad_Data_Folder".
    
- Data_Transformation: 
         Replaceing NaN by NULL and typecast all data into String format


## Data Insertion in Database:
     
- Database Table Creation and Connection: 
         Create a database with the given name passed. If the database is already created,
         open the connection to the database.

- Table creation in the database: 
         Table with name - "Good_Data", is created in the database for inserting the files 
         have in the "Good_Data_Folder" based on given column names and datatype in the schema file. If the table is already
         present, then the new table is not created and new files are inserted in the already present table as we want 
         training to be done on new as well as old training files. 

- Insertion of file in the table: 
         All the files in the "Good_Data_Folder" are inserted in the created table. 

- Delete of Good Data Folder: 
         After all data load into DB delete existing Good Data Folder.

 - Move Bad Folder File in Archive File: 
         Move the Bad Data Folder files into Archive folder and delete the bad data folder.

 - Database to CSV: 
         Fetch Data from Database and make into a CSV file and store CSV file into specified location.
         

## 🏽‍ For Prediction Dataset perform same operation like Data Validation and Data Insertion in database. 


## Model Training:
    
- Data Export from Specific Location: 
         The CSV file loaded from specific location.

- Data Preprocessing: 

         -> Drop Duplicate Rows.
        
         -> Check for null values in the columns. If present, impute the null values using Random Sample Imputation technique.
        
         -> Separately create feature from existing Feature.
        
         -> Perform LabelEncoding depend on the priority of the record.
        
    ###  Airline 
    ![Airline](https://user-images.githubusercontent.com/76595524/148056571-68d7e31d-0c80-4ba5-8099-425c4d3faa27.png)
   ###  Source
   ![source](https://user-images.githubusercontent.com/76595524/150360619-fdaaadb4-8586-4172-9b7a-4b67828c73f9.jpg)
   ###  Destination
   ![dest](https://user-images.githubusercontent.com/76595524/150360597-d93b66aa-be15-4aa7-85a7-cc6ab9a6a330.jpg)
   

- Clustering: 
        KMeans algorithm is used to find clusters in preprocessed data.  
        ![Cluster](https://user-images.githubusercontent.com/76595524/148057837-4e376515-11c4-4833-a814-5c5f3d48c768.png)

- Find best Madel:
        Depend on Cluster number we find the best model for each cluster. By using 2  algorithms “RandomForestRegressor” and "XGBoost". 
        For each cluster both the hyper tunned algorithms are used.       
        ![s4](https://user-images.githubusercontent.com/76595524/149934255-b5b0ddf8-7dcd-450e-a395-c58b320b9052.jpg)
        
   
- Predition:
        By the Existing Madel depend on Cluster number perform all operation for prediction the result except Find Best Madel step.
       
       
       
###  🏽‍ For each Stpes maintain in Log to better understand if any exception occurred, so that error can be easily identified and fixed that issue.




## Setup

## Create a file "Dockerfile" with below content

```
FROM python:3.7
COPY . /app
WORKDIR /app
RUN pip install -r requirements.txt
ENTRYPOINT [ "python" ]
CMD [ "main.py" ]
```

## Create a "Procfile" with following content
```
web: gunicorn main:app
```


## create a file ".circleci\config.yml" with following content
<a href="https://github.com/abhisheksaharaja/Flight_Price_Prediction_ML_Project/blob/main/.circleci/config.yml">.circleci\config.yml</a>


## to create requirements.txt
```
pip freeze>requirements.txt
```

## initialize git repo
```
git init
git add .
git commit -m "first commit"
git branch -M main
git remote add origin <github_url>
git push -u origin main
```

## create a account at circle ci

<a href="https://circleci.com/login/">Circle CI</a>


## setup your project 

<a href="https://app.circleci.com/projects/project-dashboard/github/abhisheksaharaja/setup/"> Setup project </a>


## Environment variable setup in Circle CI

```
DOCKERHUB_USER
DOCKER_HUB_PASSWORD_USER
HEROKU_API_KEY
HEROKU_APP_NAME
HEROKU_EMAIL_ADDRESS
DOCKER_IMAGE_NAME=flightpricepredictionml1996
```

## to update the modification

```
git add .
git commit -m "proper message"
git push 
```


## Inference demo

1. UI for Flight Price Prediction
![s1](https://user-images.githubusercontent.com/76595524/149933862-d9fca45f-5e9e-4fbd-bee7-c7458d38f653.jpg)

2. UI for Prediction File Outcome:
![s2](https://user-images.githubusercontent.com/76595524/149933876-90121044-eb88-453a-ae36-15ef260c65f7.jpg)

3. UI for Custom File Prediction Outcome:
![s3](https://user-images.githubusercontent.com/76595524/149933888-42a9033f-885d-4be4-8190-eceb63eaf9a3.jpg)  
