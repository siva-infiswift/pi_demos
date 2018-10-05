#!/usr/bin/env python3
import numpy as np
import pandas as pd
import pickle
import sklearn.preprocessing as sp

class FaultDetector:
    def __init__(self, model_file_name):
        self.loaded_model = pickle.load(open(model_file_name, 'rb'))
    def detect_fault(json):
       # Create a new dataframe
       comb_df = pd.DataFrame()

       # Scale data and create lists
       df1 = sp.MinMaxScaler().fit_transform(json)
       pos = list(df1[:,0])
       ld = list(df1[:,1])
       comb = ld + pos

       # Define the new Dataframe comb_df and append each combined list 
       comb_df = pd.DataFrame()
       comb_df = comb_df.append(comb)
       # Transpose this list and append to new Dataframe
       # shape of this dataframe will be (no.of.strokes*1000)
       comb_df = comb_df.transpose()

       # Generate prediction
       prob_scores = loaded_model.predict_proba(comb_df)
       output = loaded_model.predict(comb_df)
       return (prob_scores[0]*100)
