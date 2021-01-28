# Databricks notebook source
# MAGIC %md 
# MAGIC # Covid Data Base
# MAGIC 
# MAGIC - Felipe Gonzalez

# COMMAND ----------

# MAGIC %md 
# MAGIC The purpose odf this notebook is to build a DB to predict the daily covid cases by province in Canada. At the moment, the data considered are:
# MAGIC 
# MAGIC 1. Covid Cases
# MAGIC 2. Weather Conditions
# MAGIC 3. Population

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Covid cases DB

# COMMAND ----------

# MAGIC %md 
# MAGIC The purpose of this class is to handle the data base obteined from an url direction: Download, Save, Read

# COMMAND ----------

# MAGIC %md 
# MAGIC Do you want to update Covid Data Base (Include today's data)? 

# COMMAND ----------

update_covidDB = False
# If you wanna update covid data you must update 2021 weather data
update_2021weatherdata = False
# If this is the First time you run the script. 2020 weather data must be updated
update_2020weatherdata = False

# COMMAND ----------

import urllib.request

# Class to handle downloading data from url and storing in dbfs
class Handle_datafiles:
  def __init__(self, url, path, file):
    self.url = url
    self.path = path
    self.file = file
    
  # save file from url to dbfs
  def SaveFile_FromUrl(self):
    urllib.request.urlretrieve(self.url,"/tmp/"+self.file)
    dbutils.fs.mv("file:/tmp/"+self.file, self.path + self.file)

  # funtion to read databases from dbfs depending on number of lines we want (important for 2021 weather data)
  def ReadFile(self, n = None):
    if n is None:
        df_tmp = spark.read.csv(self.path + self.file, header=True, inferSchema= True)
    else:
        df_tmp = spark.read.csv(self.path + self.file, header=True, inferSchema= True).limit(n) # number of lines read
    return df_tmp

  # function to save in dbfs and read depending on a update conditional
  def SaveAndRead(self, update, n = None):
    if(update):
      self.SaveFile_FromUrl()
    df = self.ReadFile(n)    
    return df
  
  # save from url and read from dbfs
  def SaveAndReadFromUrl(self, n = None):
    self.SaveFile_FromUrl()
    df = self.ReadFile(n)    
    return df

# COMMAND ----------

url_covidDB = "https://health-infobase.canada.ca/src/data/covidLive/covid19-download.csv"
path_covidDB = "/FileStore/tables/Covid_DB/"
file_covidDB = "covid_data.csv"

df_covid_data = Handle_datafiles(url_covidDB, path_covidDB, file_covidDB).SaveAndRead(update_covidDB)

# COMMAND ----------

# Remove Canada and Repatriated travellers data
df_covid = df_covid_data.filter((df_covid_data.prname != 'Canada') & (df_covid_data.prname != 'Repatriated travellers'))

# COMMAND ----------

Provinces_tmp = [x.prname for x in df_covid.select("prname").distinct().collect()]
print(Provinces_tmp)

# COMMAND ----------

from pyspark.sql import SQLContext

df_covid.createOrReplaceTempView("table_prediction")
query_latest_rec = """SELECT date,prname,numtoday FROM table_covid WHERE prname = 'Quebec' ORDER BY date DESC limit 1"""
latest_rec = sqlContext.sql(query_latest_rec)
latest_rec.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Weather DataBase

# COMMAND ----------

# MAGIC %md 
# MAGIC Identifying the Station ID necessary to get weather daily data par station

# COMMAND ----------

df_stations_tmp = spark.read.csv("/FileStore/tables/Covid_DB/Weather/Station_Inventory_EN.csv", header=True, inferSchema= True)
df_stations = df_stations_tmp.select("Province","Station ID")
df_stations = df_stations.withColumnRenamed("Station ID","ID")

# Provinces format 
Provinces_stations = [x.Province for x in df_stations.select("Province").distinct().collect()]
Provinces_tmp.sort()
Provinces_stations.sort()

# COMMAND ----------

# MAGIC %md
# MAGIC Dictionary to get all the meteo stations by province

# COMMAND ----------

# Dictinoary {Province: list of stationsID}
stationsIDparProvince = {}
for i, prov in enumerate(Provinces_stations):
  #if prov == "ONTARIO"
    df_tmp = df_stations.filter(df_stations.Province == prov)
    list_tmp = [x.ID for x in df_tmp.select("ID").distinct().collect()]
    #list_tmp = [5306, 5624, 10869, 5879, 5499, 42012, 30165]
    stationsIDparProvince[Provinces_tmp[i]] = list_tmp

# COMMAND ----------

# Considering only meteo stations of most populated cities by province

stationsIDparProvince = {'New Brunswick': [6206, 6207, 50390, 54282, 6157, 71700, 10981, 71668, 6159, 6248, 6249, 6250, 71609], #13
                         'Newfoundland and Labrador':  [6718, 6719, 50089, 6720, 6721, 6722, 48871, 27115, 6933, 6643], #10 
                         'Nunavut':[1758, 47388, 42503, 71909, 1720, 47427],  #6
                         'Quebec':[8343, 5414, 5415, 51157, 26855, 5420, 5418, 30165, 20719, 53001, 5590, 8375, 5249, 
                                   5250, 51457, 5251, 5528, 5529, 5292, 52138], #20  
                         'Prince Edward Island':[6525, 6526, 6527, 6528, 10800, 6546], #6
                         'Yukon':[1616, 1617, 50842, 1618, 48168, 26988,  10194],  #7
                         'British Columbia':[819, 820, 821, 822, 823, 824, 825, 826, 827, 828, 108, 109, 110, 111, 112], # 15
                         'Saskatchewan':[3327, 3328, 46647, 46687, 3330, 3331, 3334, 3002, 3003, 3004, 3007, 3008, 3011, 3016], # 14
                         'Northwest Territories':[1706, 51058, 45467, 1707, 27338], #5
                         'Alberta':[45847, 2202, 2203, 2204, 45867, 2207, 2208, 2209, 52200, 1863, 1864, 1865, 31427, 
                                    30907, 1870, 1871, 1872, 43149, 2132, 2133], #20
                         'Nova Scotia':[6355, 6354, 6355, 49128, 43405, 43124, 43403, 50620, 6358, 6485, 6486, 6490, 6491], #13
                         'Manitoba':[3689, 3690, 3691, 3692, 3693, 27525, 3703, 3704, 3705, 28051, 3471, 3472, 3473], #13
                         'Ontario':[5051, 41863, 5052, 5053, 5054, 5055, 5056, 54239, 53678, 43203, 5065, 5066, 5067, 5068,
                                    4328, 4329, 4330, 30578, 4339, 4340, 4931, 4932, 4933, 52742, 4937]         
}

# COMMAND ----------

# MAGIC %md 
# MAGIC Store url directions for each meteo station (par year) into a Dictionary

# COMMAND ----------

# Dictinary {Province: {year: {stationID.csv: url}}} 
# There are 2 province keys: For example, Quebec, and Quebec2021
URLparProvince = {}
dic_tmp = {}
years = [2020,2021]
for k,v in stationsIDparProvince.items():  
  year_dic = {}
  for y in years:
    dic_tmp = {}
    for stationID in v:
      filename = str(stationID)+".csv"
      url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID="+str(stationID)+"&Year="+str(y)+"&Month=1&Day=14&timeframe=2"
      dic_tmp[filename] = url
    year_dic[y] = dic_tmp
  URLparProvince[k]=year_dic

# COMMAND ----------

# MAGIC %md
# MAGIC features considered in the weather database

# COMMAND ----------

#features = ["Date/Time", "Max Temp (°C)","Max Temp Flag", "Min Temp (°C)", "Min Temp Flag", "Mean Temp (°C)","Mean Temp Flag", "Heat Deg Days (°C)", "Heat Deg Days Flag", "Cool Deg Days (°C)", "Cool Deg Days Flag", "Total Rain (mm)", "Total Rain Flag", "Total Snow (cm)", "Total Snow Flag", "Total Precip (mm)", "Total Precip Flag", "Snow on Grnd (cm)", "Snow on Grnd Flag", "Dir of Max Gust (10s deg)", "Dir of Max Gust Flag","Spd of Max Gust (km/h)", "Spd of Max Gust Flag"]

features = ["Date/Time", "Max Temp (°C)", "Min Temp (°C)", "Mean Temp (°C)", "Heat Deg Days (°C)", "Cool Deg Days (°C)", "Total Rain (mm)", "Total Precip (mm)", "Snow on Grnd (cm)", "Dir of Max Gust (10s deg)", "Spd of Max Gust (km/h)"]
cols = ["Max Temp (°C)", "Min Temp (°C)", "Mean Temp (°C)", "Heat Deg Days (°C)", "Cool Deg Days (°C)", "Total Rain (mm)", "Total Precip (mm)", "Snow on Grnd (cm)", "Dir of Max Gust (10s deg)", "Spd of Max Gust (km/h)"]

# COMMAND ----------

# MAGIC %md 
# MAGIC functions to download all weather data. Group By date and province. Store data in a DF

# COMMAND ----------

import datetime 
from pyspark.sql.functions import col
import functools 
from pyspark.sql.functions import lit

# number of days during 2021 in order to take only the line we are interested in
delta = (datetime.datetime.now() - datetime.datetime.strptime("2021-01-01", "%Y-%m-%d")).days

class Handle_DFs:
  def __init__(self, list_dfs, province):
    self.list_dfs = list_dfs
    self.province = province
    
  # funtion to merge dataframes
  def unionAll_dfs(self):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), self.list_dfs)

  # function to group by date: all the features must be coverted to floats.
  def groupByDate_floats(self, features):
    df_tmp = self.unionAll_dfs()
    for col_name in features:
      df_tmp = df_tmp.withColumn(col_name, col(col_name).cast('float')) # convert to float
      df_out = df_tmp.groupBy("Date/Time").avg().orderBy("Date/Time") # groupBy Date
      df_out = df_out.withColumn("Province", lit(self.province)) # Include Province
    return df_out;
  
  # Saving dataframe into DBFS
  def SaveDF_tofile(self, df,filename):
    path_toSave_Results = filename +self.province
    df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(path_toSave_Results)
    
  # Read DF from file or downloaded results
  def ReadDF_fromfile(self, update, filename, features):
    if(update):
      df_tmp = self.groupByDate_floats(features)
      self.SaveDF_tofile(df_tmp, filename)
    else:
      df_tmp = spark.read.csv(filename + self.province, header=True, inferSchema= True)  
    return df_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC Download or read all weather data. Group By date and province. Store data in a DF.
# MAGIC 
# MAGIC At the end, we have a list of weather DF par Province

# COMMAND ----------

main_path = "/FileStore/tables/Covid_DB/Weather/"
path_to_save = main_path + "Results/"

# Deleting previous downloaded data:
if(update_2020weatherdata):
  dbutils.fs.rm(main_path + "2020", True)
  dbutils.fs.rm(path_to_save + "R2020/", True)
if(update_2021weatherdata):
  dbutils.fs.rm(main_path + "2021", True)
  dbutils.fs.rm(path_to_save + "R2021/", True)
if(update_2020weatherdata or update_2021weatherdata):
  dbutils.fs.rm(path_to_save + "RTotal/", True)

# COMMAND ----------

# For loop to built weather DB
df_weather_list = []
for k,year in URLparProvince.items():
  print("Province: ", k, " started: ",   datetime.datetime.now())
  list_df_2020 = []
  list_df_2021 = []
  if(update_2020weatherdata or update_2021weatherdata):
    # -----------
    for y,files in year.items():      
      path = main_path +str(y)+"/"
      for filename, url in files.items():
        #print("              3. KEY ", filename)
        if(y == 2021 and update_2021weatherdata):
          df_tmp = Handle_datafiles(url, path, filename).SaveAndReadFromUrl(delta)
          list_df_2021.append(df_tmp.select(features))
        elif(y == 2020 and update_2020weatherdata):
          df_tmp = Handle_datafiles(url, path, filename).SaveAndReadFromUrl()
          list_df_2020.append(df_tmp.select(features))
    # -----------
    
    df_2020 = Handle_DFs(list_df_2020, k).ReadDF_fromfile(update_2020weatherdata, path_to_save + "R2020/", cols)
    df_2021 = Handle_DFs(list_df_2021, k).ReadDF_fromfile(update_2021weatherdata, path_to_save + "R2021/", cols)
    
    df_weather_byProv = Handle_DFs([df_2020,df_2021], k).unionAll_dfs() 
    Handle_DFs(df_weather_byProv, k).SaveDF_tofile(df_weather_byProv, path_to_save + "RTotal/")          
  else:     
    df_weather_byProv = spark.read.csv(path_to_save+"RTotal/"+k, header=True, inferSchema= True)
    
  df_weather_list.append(df_weather_byProv)    
  print(k, "  ==  Stored in file and in List == ",   datetime.datetime.now())    

# COMMAND ----------

# MAGIC %md
# MAGIC Merge all weather DF

# COMMAND ----------

df_weather = Handle_DFs(df_weather_list, k).unionAll_dfs() 

old_colnames = df_weather.columns
new_colnames = ["Date","MaxTemp", "MinTemp", "MeanTemp", "HeatDegDays", "CoolDegDays", "TotalRain", "TotalPrecip", "SnowOnGrnd", "DirOfMaxGust", "SpdOfMaxGust", "Province"]

for i,j in enumerate(old_colnames):
  df_weather = df_weather.withColumnRenamed(j,new_colnames[i])
  
df_weather.show()

# COMMAND ----------

df_weather.createOrReplaceTempView("table_weather")
query_latest_rec = """SELECT * FROM table_weather """
latest_rec = sqlContext.sql(query_latest_rec)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Population Data Base

# COMMAND ----------

df_population_data = spark.read.csv("/FileStore/tables/Covid_DB/Population/population.csv", header=True, inferSchema= True)

# COMMAND ----------

from pyspark.sql.functions import when

df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "NS", "Nova Scotia").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "NL", "Newfoundland and Labrador").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "NT", "Northwest Territories").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "QC", "Quebec").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "BC", "British Columbia").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "MB", "Manitoba").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "NU", "Nunavut").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "ON", "Ontario").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "SK", "Saskatchewan").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "AB", "Alberta").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "PE", "Prince Edward Island").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "YT", "Yukon").otherwise(df_population_data["ProvinceAvr"]))
df_population_data = df_population_data.withColumn("ProvinceAvr", when(df_population_data["ProvinceAvr"] == "NB", "New Brunswick").otherwise(df_population_data["ProvinceAvr"]))

# COMMAND ----------

old_colnames = df_population_data.columns
new_colnames = ['Province', 'TousAges', 'ans0_4', 'ans5_9', 'ans10_14', 'ans15_19', 'ans20_24', 'ans25_29', 'ans30_34', 'ans35_39', 'ans40_44', 'ans45_49', 'ans50_54', 'ans55_59', 'ans60_64', 'ans65_69', 'ans70_74', 'ans75_79', 'ans80_84', 'ans85_89', 'ans90_94', 'ans95_99',  'ans100etplus', 'AgeMedian']

for i,j in enumerate(old_colnames):
  df_population_data = df_population_data.withColumnRenamed(j,new_colnames[i])
  
df_population_data.show()

# COMMAND ----------

df_population_data.createOrReplaceTempView("table_population")
query_latest_rec = """SELECT * FROM table_population"""
latest_rec = sqlContext.sql(query_latest_rec)
#latest_rec.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Join with Covid Data

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Join Covid and population Data

# COMMAND ----------

df_covid.createOrReplaceTempView("table_covid")
df_population_data.createOrReplaceTempView("table_population")
query_latest_rec = """SELECT tc.pruid, tc.prname, tc.date, tc.numtotal, tc.numtoday, tc.numprob, tc.numdeaths, tc.numtotal, tc.numtested, tc.numrecover, tc.percentrecover, tc.ratetested, tc.percentoday, tc.ratetotal, tc.ratedeaths, tc.numdeathstoday, tc.percentdeath, tc.numtestedtoday, tc.numrecoveredtoday, tc.percentactive,  tc.numactive, tc.rateactive,  tc.numtotal_last14, tc.ratetotal_last14, tc.numdeaths_last14, tc.ratedeaths_last14, tc.numtotal_last7, tc.ratetotal_last7, tc.numdeaths_last7, tc.ratedeaths_last7, tc.avgtotal_last7, tc.avgincidence_last7, tc.avgdeaths_last7, tc.avgratedeaths_last7, tp.TousAges, tp.ans0_4, tp.ans5_9, tp.ans10_14, tp.ans15_19, tp.ans20_24, tp.ans25_29, tp.ans30_34, tp.ans35_39, tp.ans40_44, tp.ans45_49, tp.ans50_54, tp.ans55_59, tp.ans60_64, tp.ans65_69, tp.ans70_74, tp.ans75_79, tp.ans80_84, tp.ans85_89, tp.ans90_94, tp.ans95_99, tp.ans100etplus, tp.AgeMedian
  FROM table_population tp  FULL JOIN table_covid tc
    ON tp.Province = tc.prname"""
df_covid_population = sqlContext.sql(query_latest_rec)
df_covid_population.show(2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Join (Covid+population) and Weather Data

# COMMAND ----------

df_covid_population.createOrReplaceTempView("table_covid_population")
query_latest_rec = """SELECT tcp.pruid, tcp.prname, tcp.date, tcp.numtoday, tcp.numprob, tcp.numdeaths, tcp.numtotal, tcp.numtested, tcp.numrecover, tcp.percentrecover, tcp.ratetested, tcp.percentoday, tcp.ratetotal, tcp.ratedeaths, tcp.numdeathstoday, tcp.percentdeath, tcp.numtestedtoday, tcp.numrecoveredtoday, tcp.percentactive,  tcp.numactive, tcp.rateactive,  tcp.numtotal_last14, tcp.ratetotal_last14, tcp.numdeaths_last14, tcp.ratedeaths_last14, tcp.numtotal_last7, tcp.ratetotal_last7, tcp.numdeaths_last7, tcp.ratedeaths_last7, tcp.avgtotal_last7, tcp.avgincidence_last7, tcp.avgdeaths_last7, tcp.avgratedeaths_last7, tcp.TousAges, tcp.ans0_4, tcp.ans5_9, tcp.ans10_14, tcp.ans15_19, tcp.ans20_24, tcp.ans25_29, tcp.ans30_34, tcp.ans35_39, tcp.ans40_44, tcp.ans45_49, tcp.ans50_54, tcp.ans55_59, tcp.ans60_64, tcp.ans65_69, tcp.ans70_74, tcp.ans75_79, tcp.ans80_84, tcp.ans85_89, tcp.ans90_94, tcp.ans95_99, tcp.ans100etplus, tcp.AgeMedian, tw.MaxTemp, tw.MinTemp, tw.MeanTemp, tw.HeatDegDays, tw.CoolDegDays, tw.TotalRain, tw.TotalPrecip, tw.SnowOnGrnd, tw.DirOfMaxGust, tw.SpdOfMaxGust
FROM table_covid_population tcp
INNER JOIN  table_weather tw 
ON tcp.date = tw.Date AND tcp.prname = tw.Province
"""

df_COVID = sqlContext.sql(query_latest_rec)
df_COVID.show(2)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Save Full DF to File

# COMMAND ----------

dbutils.fs.rm("/FileStore/tables/Covid_DB/Total_Result/", True)
path_toSave = "dbfs:/FileStore/tables/Covid_DB/Total_Result/"
df_COVID.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(path_toSave)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Save DF to File last row for predicting

# COMMAND ----------

df_COVID.createOrReplaceTempView("table_prediction")
query_latest_rec = """SELECT * FROM table_prediction ORDER BY date DESC LIMIT 13"""
df_predict = sqlContext.sql(query_latest_rec)

dbutils.fs.rm("/FileStore/tables/Covid_DB/ForPrediction/", True)
path_toSave = "dbfs:/FileStore/tables/Covid_DB/ForPrediction/"
df_predict.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(path_toSave)

# COMMAND ----------

query_province_id = """SELECT pruid, prname FROM table_prediction ORDER BY date DESC LIMIT 13"""
province_id = sqlContext.sql(query_province_id)
province_id.show()

# COMMAND ----------


