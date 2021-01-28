========================
OBJECTIVE
========================

The aim of this project is to predict the daily covid cases by province in Canada.
A particular purpose is to research if there is any influence of the weather on the covid numbers.

========================
DATA BASES
========================

At the moment, the considered databases are:
   * Covid DataBases: https://www.canada.ca/en/public-health/services/diseases/2019-novel-coronavirus-infection.html?&utm_campaign=gc-hc-sc-coronavirus2021-ao-2021-0005-9834796012&utm_medium=search&utm_source=google_grant-ads-107802327544&utm_content=text-en-434601690164&utm_term=covid

   * Weather Conditions: https://climate.weather.gc.ca/historical_data/search_historic_data_e.html

   * Population: https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1710000901


========================
PYSPARK NOTEBOOKS
========================

With this purpose, there are two notebooks:

     1. COVID_PROJECT_DATABASE: To built the entire dataset with the databases mentioned above
     2. Covid_Project_ML: To predict the number of cases by province

==========================
TO RUN THE PROJECT:
==========================

      1. First upload the datasets in DBFS DataBricks:
     	- DB/Station_Inventory_EN.csv   -> "/FileStore/tables/Covid_DB/Weather/Station_Inventory_EN.csv"
	- DB/population.csv  -> "/FileStore/tables/Covid_DB/Population/population.csv"

      2. Open COVID_PROJECT_DATABASE using Databricks:
      	 -If you are running for the first time, Set these variables to True:
      	 update_covidDB = True
	 update_2021weatherdata = True
	 update_2020weatherdata = True
	 - Run all the project

      3. Open Covid_Project_ML using Databricks:
      	 Run all cells. The notebook will predict the todays cases (that will be reported on the next day).
	 Don't pay attention to the dates in the markdown cells
	 
	 
      





     


