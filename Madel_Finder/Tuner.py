#doing all necessary import

from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.model_selection import RandomizedSearchCV,GridSearchCV
from sklearn import metrics
import numpy as num

class MadelTuner:

    """
             This class shall  be used to find the model with R_squre and Adj_r_square
    """

    def __init__(self,log,file_object):
        self.log=log
        self.file_object=file_object
        self.RandomForest=RandomForestRegressor()
        self.xgb=XGBRegressor(objective='reg:squarederror')

    def best_param_XgbRegressor(self, x_train, y_train):

        """
                                      Method Name: best_param_XgbRegressor
                                      Description: Get the parameters for XGBoost Algorithm which give the best
                                                   accuracy. Use Hyper Parameter Tuning.
                                      Output: The model with the best parameters
                                      On Failure: Raise Exception
        """

        self.log.log(self.file_object, 'Entered in best_param_XgbRegressor method of MadelTuner Class. Start finding best Madel')
        try:

            # initializing with different combination of parameters
            self.params = {'max_depth': [1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13],
                      'learning_rate': [0.001,0.0001,0.0002,0.003,0.1,0.002,0.0003],
                      'n_estimators': [i for i in range(400, 2000)],
                      'reg_lambda': [0.001, 0.1, 1.0, 10.0, 19.0, 29.0, 35.0, 45.0, 60.0],
                      'colsample_bytree': [0.3, 0.4, 0.2, 0.1]
                      }

            # Creating an object of the Grid Search class
            self.xgb_Madel = RandomizedSearchCV(estimator=self.xgb, param_distributions=self.params, n_iter=30, cv=5, n_jobs=1, verbose=2)

            # finding the best parameters
            self.xgb_Madel.fit(x_train, y_train)

            # extracting the best parameters
            self.max_depth=self.xgb_Madel.best_params_['max_depth']
            self.learning_rate = self.xgb_Madel.best_params_['learning_rate']
            self.n_estimators = self.xgb_Madel.best_params_['n_estimators']
            self.reg_lambda = self.xgb_Madel.best_params_['reg_lambda']
            self.colsample_bytree = self.xgb_Madel.best_params_['colsample_bytree']

            # creating a new model with the best parameters
            self.XgbRegressorMadel=XGBRegressor(max_depth=self.max_depth,learning_rate=self.learning_rate,
                                                n_estimators=self.n_estimators,reg_lambda=self.reg_lambda,
                                                colsample_bytree=self.colsample_bytree)

            # training the mew model
            self.XgbRegressorMadel.fit(x_train,y_train)
            self.log.log(self.file_object,'XgbRegressor best params: ' + str(self.xgb_Madel.best_params_) + '. Exited the best_param_XgbRegressor method of the MadelTuner class')
            return self.XgbRegressorMadel

        except Exception as e:
            self.log.log(self.file_object,'Getting an Error while Find The XgbRegressor Madel for the best_param_XgbRegressor method of MadelTuner class. Error Msg: ' + str(e))
            raise e


    def best_param_Random_Forest(self,x_train,y_train):

        """
                                        Method Name: best_param_Random_Forest
                                        Description: Get the parameters for Random Forest Algorithm which give
                                                     the best accuracy. Using Hyper Parameter Tuning.
                                        Output: The model with the best parameters
                                        On Failure: Raise Exception
        """

        self.log.log(self.file_object,'Enter in best_param_Random_Forest method of MadelTuner Class. Start finding best Madel')
        try:

            # initializing with different combination of parameters
            self.grid={ 'n_estimators': [f for f in range(650, 2000)],
                    'max_features': ['auto', 'sqrt'],
                    'max_depth': [f for f in range(10, 30)],
                    'min_samples_split': [10, 15, 20, 27, 32, 35, 37],
                    'min_samples_leaf': [1, 2, 5, 7, 10]
                   }

            # Creating an object of the Grid Search class
            self.rf_random = RandomizedSearchCV(estimator=self.RandomForest, param_distributions=self.grid, n_iter=10, cv=5, n_jobs=1)

            # finding the best parameters
            self.rf_random.fit(x_train, y_train)

            # extracting the best parameters
            self.n_estimators=self.rf_random.best_params_['n_estimators']
            self.max_features=self.rf_random.best_params_['max_features']
            self.max_depth=self.rf_random.best_params_['max_depth']
            self.min_samples_split=self.rf_random.best_params_['min_samples_split']
            self.min_samples_leaf=self.rf_random.best_params_['min_samples_leaf']

            # creating a new model with the best parameters
            self.Random_Forest_Madel=RandomForestRegressor(n_estimators=self.n_estimators, max_features=self.max_features,
                                                           max_depth=self.max_depth, min_samples_split=self.min_samples_split,
                                                           min_samples_leaf=self.min_samples_leaf)

            # training the mew model
            self.Random_Forest_Madel.fit(x_train,y_train)
            self.log.log(self.file_object,'Random Forest best params: '+str(self.rf_random.best_params_)+'. Exited the best_param_Random_Forest method of the MadelTuner class')
            return self.Random_Forest_Madel

        except Exception as e:
            self.log.log(self.file_object,'Getting an Error while Find The Random Forest Regressor Madel for the best_param_Random_Forest method of MadelTuner class. Error Msg: '+str(e))
            raise e

    def adj_r_square(self, r_square, x_test):
        self.r_square = r_square
        self.x_test=x_test
        self.n = self.x_test.shape[0]
        self.p = self.x_test.shape[1]
        adj_r_square = (1 - (((1 - self.r_square) * (self.n - 1)) / (self.n - self.p - 1)))
        return adj_r_square


    def find_best_madel(self,x_train,y_train,x_test,y_test):

        """
                                        Method Name: find_best_madel
                                        Description: Find out the Model which has the best r_square result.
                                        Output: The best model name and the model object
                                        On Failure: Raise Exception
        """
        self.log.log(self.file_object,'Entered in find_best_madel method of MadelTuner Class. To find best madel after comparision.')
        try:

            # create best model for Random Forest
            self.Random_Forest_Madel=self.best_param_Random_Forest(x_train,y_train)
            self.prediction_Random_Forest=self.Random_Forest_Madel.predict(x_test)

            self.r_square_rf=metrics.r2_score(y_test,self.prediction_Random_Forest)
            self.adj_r_square_rf=self.adj_r_square(self.r_square_rf,x_test)
            self.log.log(self.file_object, 'R_Square for Random Forest:' + str(self.r_square_rf)+'Adjusted_R_Square for Random Forest: '+str(self.adj_r_square_rf))

            # create best model for XGBoost
            self.Xgb_Madel = self.best_param_XgbRegressor(x_train, y_train)
            self.prediction_XgbRegressor = self.Xgb_Madel.predict(x_test)

            self.r_square_xgb = metrics.r2_score(y_test, self.prediction_XgbRegressor)
            self.adj_r_square_xgb = self.adj_r_square(self.r_square_xgb, x_test)
            self.log.log(self.file_object, 'R_Square for XGBRegressor:' + str(self.r_square_xgb) + 'Adjusted_R_Square for XGBRegressor: ' + str(self.adj_r_square_xgb))

            # comparing the two models
            if self.r_square_rf < self.r_square_xgb:
                return 'Xgb_Madel',self.Xgb_Madel
            else:
                return 'Random_Forest_Madel',self.Random_Forest_Madel

        except Exception as e:
            self.log.log(self.file_object, 'Madel Selection Failed!! Error Occoured While Find the Best Madel in find_best_madel method of MadelTuner Class. Error Msr: '+str(e))
            raise e
