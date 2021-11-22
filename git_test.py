# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 08:48:35 2020

@author: shanmukh.rajgire

"""

import pandas as pd

from sklearn.ensemble import RandomForestRegressor
s3 = boto3.resource(u's3')
s3_bucket="abg-ml-data"
input_key="Sample_input_data.csv"
test_file_key = "Sample_testing_data.csv"
output_key="Sample_pred_output_sg.csv"
bucket = s3.Bucket(s3_bucket)

#get input file from s3 using boto3
obj = bucket.Object(key=input_key)
response = obj.get()
lines = response[u'Body']
data = pd.read_csv(lines) 

# input_filename = "Sample_input_data.csv"
# data = pd.read_csv(input_filename)

x=data.drop( columns = ['% CE','Date'])

y=data[['% CE']]

regressor = RandomForestRegressor() 
  
regressor.fit(x, y) 

#get test file from s3 using boto3
obj = bucket.Object(key=test_file_key)
response = obj.get()
lines = response[u'Body']
data_t = pd.read_csv(lines) 

# test_filename = "Sample_testing_data.csv"
# data_t = pd.read_csv(test_filename)

data_test=data_t.drop( columns = ['Date'])

y_pred = regressor.predict(data_test)

#buffer creation to upload output csv back to s3
csv_buffer = StringIO()

res = pd.DataFrame(y_pred)

res.columns = ["pred_CE"]
res.to_csv(csv_buffer,index=False,header=True)

#upload outfile back to s3
s3.Object(s3_bucket, output_key).put(Body=csv_buffer.getvalue()) 
