import json
from flask import Flask, render_template, request, Response
from Training_Validation_Insertion import Training_Validation
from Train_Madel import TrainingMadel
from Prediction_Validation_Insertion import Predict_Validation
from Prediction_Madel import predictionMadel
from flask_cors import CORS, cross_origin

from wsgiref import simple_server
import flask_monitoringdashboard as dashboard
import os


os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)

@app.route('/', methods=['GET'])
@cross_origin()
def index():
    return render_template('index.html')


@app.route('/predict', methods=['POST'])
@cross_origin()
def PredictClient():
    try:
        if request.form is not None:
            path=request.form['filepath']
            predict_validation_obj=Predict_Validation(path)
            predict_validation_obj.Prediction()

            pred=predictionMadel(path)
            predict_path, json_predictions=pred.madelPrediction()
            return Response("Prediction File created at !!!" + str(predict_path) + 'and few of the predictions are ' + str(json.loads(json_predictions)))

        elif request.json is not None:
            path=request.json['filepath']
            predict_validation_obj=Predict_Validation(path)
            predict_validation_obj.Prediction()

            pred=predictionMadel(path)
            predict_path, json_predictions=pred.madelPrediction()
            return Response("Prediction File created at !!!" + str(predict_path) + 'and few of the predictions are ' + str(json.loads(json_predictions)))

        else:
            print('Nothing Matched')
    except ValueError:
        return Response("Error Occurred! ValueError")
    except KeyError:
        return Response("Error Occurred! KeyError")
    except Exception as e:
        return Response("Error Occurred!! Exception")


@app.route("/train", methods=['GET', 'POST'])
@cross_origin()
def trainRouteClient():
    try:
        folder_path='Training_Batch_Files'
        if folder_path is not None:
            path=folder_path
            train_validation_obj=Training_Validation(path)
            train_validation_obj.Training()

            train_Madel_obj=TrainingMadel()
            train_Madel_obj.MadelTraining()

    except ValueError:
        return Response('Error Occurred ValueError')
    except KeyError:
        return Response('Error Occurred KeyError')
    except Exception:
        return Response('Error Occurred Exception')
    return Response('Training Successful')





port = int(os.getenv("PORT", 6000))
if __name__ == "__main__":
    host = '0.0.0.0'
    httpd = simple_server.make_server(host, port, app)
    httpd.serve_forever()

