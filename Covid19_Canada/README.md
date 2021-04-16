========================
OBJECTIVE
========================

The aim of this project is to predict the daily covid cases by province in Canada.
The particular purpose is to research if there is any influence of the weather on the covid numbers.

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

With this purpose, there are three notebooks:

     1. 1_Projet_Covid19-Collecte_et_Integration_des_donnees.ipynb: To built the entire dataset with the databases mentioned above.
     2. 2_Projet_Covid19_Traitement.ipynb: To clean and analyze the data.
     3. 3_Projet_Covid19_Developpement_du_modele.ipynb: To predict the number of cases by province

==========================
TO RUN THE PROJECT:
==========================

      1. First upload the datasets in DBFS DataBricks:
     	- DB/Station_Inventory_EN.csv   -> "/FileStore/tables/Covid_DB/Weather/Station_Inventory_EN.csv"
	- DB/population.csv  -> "/FileStore/tables/Covid_DB/Population/population.csv"

      2. Open the 1_Projet_Covid19-Collecte_et_Integration_des_donnees module using Databricks:
      	 -If you are running for the first time, Set these variables to True:
      	 update_covidDB = True
	 update_2021weatherdata = True
	 update_2020weatherdata = True
	 - Run all the project

      3. Open 2_Projet_Covid19_Traitement.ipynb
      	 - Run all the project

      4. Open 3_Projet_Covid19_Developpement_du_modele.ipynb using Databricks:
      	 Run all cells. The notebook will predict the todays cases (that will be reported on the next day).
	 Don't pay attention to the dates in the markdown cells
	 
	 
      





     


