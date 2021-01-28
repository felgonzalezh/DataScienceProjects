# Databricks notebook source
# MAGIC %md
# MAGIC # Projet Covid
# MAGIC - Felipe Gonzalez

# COMMAND ----------

# MAGIC %md
# MAGIC The aim of the proyect is to predict daily covid19 cases by province in Canada, and look for any possible relation with weather conditions.
# MAGIC Our target variable is named "numtoday"

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 1. Chargement du donees

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
data_df = spark.read.csv("/FileStore/tables/Covid_DB/Total_Result", header = True, inferSchema = True)
data_df_forPrediction = spark.read.csv("/FileStore/tables/Covid_DB/ForPrediction", header = True, inferSchema = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Traitement

# COMMAND ----------

# MAGIC %md 
# MAGIC conversion de la variable date en variable numérique

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType

# Convesrion de String a Date
func =  udf (lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
df = data_df.withColumn('date', func(col('date')))
df_predict_tmp = data_df_forPrediction.withColumn('date', func(col('date')))
# convesrion de Date a Numerique
func_2 =  udf (lambda x: datetime.toordinal(x))
df = df.withColumn('date_num', func_2(col('date')))
df_predict_tmp = df_predict_tmp.withColumn('date_num', func_2(col('date')))

date_min_value = df.agg({"date_num": "min"}).collect()[0][0]
date_max_value = df.agg({"date_num": "max"}).collect()[0][0]
ndays = int(date_max_value) - int(date_min_value) 
print("Min date num: ", 0)
print("n days: ", ndays)
df = df.withColumn('date_num', col('date_num') - date_min_value)
df_predict_tmp = df_predict_tmp.withColumn('date_num', col('date_num') - date_min_value)

# COMMAND ----------

#features = ['date', 'pruid', 'Province', 'date_num', 'MaxTemp', 'MinTemp', 'MeanTemp', 'numtotal', 'TousAges', 'numtoday']

#features = ['pruid',  'prname', 'date', 'date_num', 'numtoday', 'numprob', 'numdeaths', 'numtotal', 'numtested', 'numrecover', 'percentrecover', 'ratetested', 'percentoday', 'ratetotal', 'ratedeaths', 'numdeathstoday', 'percentdeath', 'numtestedtoday', 'numrecoveredtoday',  'percentactive', 'numactive', 'rateactive', 'numtotal_last14', 'ratetotal_last14', 'numdeaths_last14', 'ratedeaths_last14', 'numtotal_last7', 'ratetotal_last7', 'numdeaths_last7', 'ratedeaths_last7', 'avgtotal_last7', 'avgincidence_last7', 'avgdeaths_last7', 'avgratedeaths_last7',  'TousAges', 'ans0_4', 'ans5_9', 'ans10_14', 'ans15_19', 'ans20_24', 'ans25_29', 'ans30_34', 'ans35_39', 'ans40_44', 'ans45_49', 'ans50_54',  'ans55_59', 'ans60_64', 'ans65_69', 'ans70_74', 'ans75_79', 'ans80_84', 'ans85_89', 'ans90_94', 'ans95_99', 'ans100etplus', 'AgeMedian', 'MaxTemp', 'MinTemp', 'MeanTemp', 'HeatDegDays', 'CoolDegDays', 'TotalRain', 'TotalPrecip', 'SnowOnGrnd', 'DirOfMaxGust', 'SpdOfMaxGust', 'Province']

#df_tmp_population = df.select(features) 

# COMMAND ----------

# MAGIC %md
# MAGIC null values are set to zero. Which is ok for covid data but it's not for weather columns (must be improved)

# COMMAND ----------

df = df.fillna(0.0)
df_predict_tmp = df_predict_tmp.fillna(0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Procesus d'apprendisage

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.1 Selection des predicteurs

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Correlation Matrix

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import round


def collerationmatrix(df, columns):
  vector_col = "corr_features"
  assembler = VectorAssembler(inputCols=columns, 
                              outputCol=vector_col)
  myGraph_vector = assembler.transform(df).select(vector_col)
  matrix = Correlation.corr(myGraph_vector, vector_col).collect()[0][0]
  corrmatrix = matrix.toArray().tolist()
  df_tmp = spark.createDataFrame(corrmatrix,columns)
  for col in columns:
    df_tmp = df_tmp.withColumn(col, round(df_tmp[col], 2)) 
  df_tmp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Correlation pupulation and numtoday

# COMMAND ----------

features_pop = ['pruid', 'TousAges', 'ans80_84', 'ans85_89', 'ans90_94', 'ans95_99', 'ans100etplus', 'AgeMedian', 'numtoday']
                #'TousAges',  'ans5_9', 'ans10_14', 'ans15_19', 'ans20_24', 'ans25_29', 'ans30_34', 'ans35_39', 'ans40_44', 'ans45_49', 'ans50_54',  'ans55_59', 'ans60_64', 'ans65_69', 'ans70_74', 'ans75_79',  ]
df_tmp_population = df.select(features_pop)   
df_tmp = collerationmatrix(df_tmp_population, features_pop)

# COMMAND ----------

# MAGIC %md
# MAGIC Correlations between age variables and numtoday (target variable) are very similar. Then, we can consider one of them ("TousAges")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Correlation weather and numtoday

# COMMAND ----------

features_weather = ['pruid', "date_num", 'MaxTemp', 'MinTemp', 'MeanTemp', 'HeatDegDays', 'CoolDegDays', 'TotalRain', 'TotalPrecip', 'SnowOnGrnd', 'DirOfMaxGust', 'SpdOfMaxGust', 'numtoday']
                
df_tmp_weather = df.select(features_weather)   
df_tmp = collerationmatrix(df_tmp_weather, features_weather)

# COMMAND ----------

# MAGIC %md
# MAGIC There is not a good correlation between meteo variables and numtoday. We could consider MeanTemp and HeatDegDays, whose correlation with numtoday are -0.15 and 0.18, although they would not have any important impact on the prediction. There is not a considerable impact of the weather on the covid dairly cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Correlation covidDB and numtoday

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Some insights:\
# MAGIC dairly variables will not be considered since we will not know that information for the day we want to predict. So, we do not consider:
# MAGIC 
# MAGIC ['percentoday','numdeathstoday', 'numtestedtoday', 'numrecoveredtoday',]
# MAGIC 
# MAGIC Rate, average, and percent variables are not considered since they are calculated:
# MAGIC 
# MAGIC ['ratetested', 'ratetotal', 'ratedeaths', 'percentdeath', 'percentactive','ratetotal_last14', 'ratedeaths_last14', 'ratetotal_last7', 'ratedeaths_last7', 'avgtotal_last7', 'avgincidence_last7', 'avgdeaths_last7', 'avgratedeaths_last7', 'rateactive'] 

# COMMAND ----------

features_covidDB = ['pruid', 'numprob', 'numdeaths', 'numtotal', 'numtested', 'numrecover', 'numactive', 'numtotal_last14', 'numdeaths_last14',  'numtotal_last7',  'numdeaths_last7',  'numtoday']
df_tmp_covidDB = df.select(features_covidDB)   
df_tmp = collerationmatrix(df_tmp_covidDB, features_covidDB)

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting predictors

# COMMAND ----------

variables = ['pruid', 'date_num', 'numdeaths', 'numtotal', 'numtested', 'numrecover', 'numactive', 'numtotal_last14', 'numdeaths_last14',  'numtotal_last7',  'numdeaths_last7', 'MeanTemp', 'HeatDegDays', 'TousAges', 'numtoday']

df_tmp = df.select(variables)

predicteurs = variables[:-1]

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(inputCols=predicteurs, outputCol="predicteurs")
df_covid = assembler.transform(df_tmp)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.2 Split Data

# COMMAND ----------

train, test = df_covid.randomSplit([0.8, 0.2])
#train.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 3.3 Entraînement du modèle (linear)

# COMMAND ----------

from pyspark.ml.regression import LinearRegression
lr_model = LinearRegression(featuresCol="predicteurs",labelCol="numtoday")
model = lr_model.fit(train)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Evaluation du model

# COMMAND ----------

evaluation_summary = model.evaluate(test)
print("MAE:{}".format(evaluation_summary.meanAbsoluteError))
print("RMSE:{}".format(evaluation_summary.rootMeanSquaredError))
print("R-squared:{}".format(evaluation_summary.r2))

# COMMAND ----------

n = len(predicteurs)

predictions = model.transform(test)
predictions.select(predictions.columns[n::2]).show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction

# COMMAND ----------

# MAGIC %md 
# MAGIC Prediction au Quebec le 27 janvier 2021. January 26th we can predict

# COMMAND ----------

data_df_forPrediction = df_predict_tmp.select(predicteurs)
day_to_predict = ndays+1

from pyspark.sql.functions import lit
data_df_forPrediction = data_df_forPrediction.withColumn("date_num", lit(day_to_predict)) # Modify by today's correspondant number
quebec_predict = data_df_forPrediction.filter(data_df_forPrediction.pruid == 24)

assembler_predict = VectorAssembler(inputCols=predicteurs, outputCol="predicteurs")
df_predict = assembler_predict.transform(quebec_predict)
predictions_t = model.transform(df_predict)

predictions_t.select(predictions_t.columns[n::1]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Quebec Real value is 1328 which means it is a really good prediction. However: 

# COMMAND ----------

# Ontario:

#ontario_predict = data_df_forPrediction.filter(data_df_forPrediction.pruid == 35)
total_predict = data_df_forPrediction

assembler_predict = VectorAssembler(inputCols=predicteurs, outputCol="predicteurs")
df_predict = assembler_predict.transform(total_predict)
predictions_t = model.transform(df_predict)

predictions_t.select(predictions_t.columns[0::n+1]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC | id | Province | Real | Pred |  
# MAGIC | 60 | Yukon    |   70 |   10 |  
# MAGIC | 59 | BC       |  485 |  478 |  
# MAGIC | 48 | Alberta  |  459 |  560 |  
# MAGIC | 47 | Saskatch |    0 |  236 |  
# MAGIC | 46 | Manitoba |   94 |  176 |  
# MAGIC | 35 | Ontario  | 1786 | 2127 |  
# MAGIC | 24 | Quebec   | 1328 | 1328 |  
# MAGIC | 10 | NewFound |    2 |    5 |  
# MAGIC | 13 | New Brun |   14 |   25 |  
# MAGIC | 12 | NS       |    0 |    6 |  
# MAGIC | 11 | P. Eduar |    0 |    4 |  
# MAGIC | 61 | Northw T |    0 |   11 |  
# MAGIC | 62 | Nunavut  |    0 |   12 |  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Manually:

# COMMAND ----------

variables = ['pruid', 'date_num', 'numdeaths', 'numtotal', 'numtested', 
             'numrecover', 'numactive', 'numtotal_last14', 'numdeaths_last14',  
             'numtotal_last7', 'numdeaths_last7', 'MeanTemp', 'TousAges', 'HeatDegDays']

X_quebec_05012021 = [[24, 362, 9577, 256002, 2695925, 
                      230803, 15622, 23378, 797,
                      10268, 435, -15, 8574571, 5.7]]
                      
df_p  = spark.createDataFrame(X_quebec_05012021, variables)

assembler_predict = VectorAssembler(inputCols=predicteurs, outputCol="predicteurs")
df_predict = assembler_predict.transform(df_p)
predictions_t = model.transform(df_predict)

predictions_t.select(predictions_t.columns[1::1]).show()

# COMMAND ----------

#pruid,prname,prnameFR,date,update,numconf,numprob,numdeaths,numtotal,numtested,
#numrecover,percentrecover,ratetested,numtoday,percentoday,ratetotal,ratedeaths,numdeathstoday,percentdeath,numtestedtoday,
#numrecoveredtoday,percentactive,numactive,rateactive,numtotal_last14,ratetotal_last14,numdeaths_last14,ratedeaths_last14,numtotal_last7,ratetotal_last7,
#numdeaths_last7,ratedeaths_last7,avgtotal_last7,avgincidence_last7,avgdeaths_last7,avgratedeaths_last7


#24,Quebec,Québec,2021-01-26,1,256002,0,9577,256002,2695925,
#230803,90.16,317730,1166,0.46,3017.12,112.87,56,3.74,0,
#1916,6.10,15622,184.11,23378,275.52,797,9.39,10268,121.01,
#435,5.13,1467,17.29,62,0.73


# COMMAND ----------


