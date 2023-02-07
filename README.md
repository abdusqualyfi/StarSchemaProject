# StarSchemaProject
![GitHub last commit](https://img.shields.io/github/last-commit/abdusqualyfi/StarSchemaProject)
![GitHub commit activity](https://img.shields.io/github/commit-activity/w/abdusqualyfi/StarSchemaProject)

[Design](#design) •
[Implementation](#implementation) •
[Evidence](#evidence)

Hi all! This project uses raw data from a bike sharing program, converts and manipulates it to a star schema format to provide useful querying to answer business questions. The files in this repository are used in the project, therefore any changes made here will also affect running this project in Databricks.

This project uses a total of 8 notebooks - 6 of which run automatically via a workflow. The first notebook N0 creates the schema for the bronze, silver and gold notebooks. It is also used in the final notebook, AutomatedTests to assert if the schema of the final tables are correct. The second notebook N1 destroys any schema or database in a specified folder to ensure it is ready for rebuilding, which is what the third notebook N2 does. Notebooks N3-N5 use the Medallion Architecture to ingest, cleanse, manipulate and transform data. Notebook N6 answers specified business questions and the final notebook N7 runs asserts to confirm the data is correct and valid.

   <details>
   <summary>Notebooks</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/abdusqualyfi/StarSchemaProject/main/img/notebooks.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>

## Design
3 database designs were made for this project - Conceptual, Logical and Physical.

   <details>
   <summary>Conceptual Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/abdusqualyfi/StarSchemaProject/main/img/conceptual_model.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540">
   ></p>
   >
   
   </details>

   <details>
   <summary>Logical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/abdusqualyfi/StarSchemaProject/main/img/logical_model.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>Physical Database Design</summary>

   ><p align="center">
   ><img src="https://raw.githubusercontent.com/abdusqualyfi/StarSchemaProject/main/img/physical_model.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   >  width="960" height="540"
   ></p>
   >
   
   </details>

## Implementation
This repo contains 4 zip files in the `files` folder and each zip file contains 1 csv file. Before extracting this, we must first define the schema for the notebooks that use the Medallion Architecture. This is done with the `N0_SchemaCreation.py` file. Note: all notebooks can be found in this repository, under the `notebooks` folder.

The next notebook  `N1_DestroySchemas.py` contains two lines that delete the main folder we will be using to store all data for this project. The code used in this notebook are as follows:
```
main_folder = "/tmp/Abdus/"
dbutils.fs.rm(main_folder, True)
```
As you can see we will be using the DBFS directory `/tmp/Abdus/` to store all files during the transformation process.

Using the schema specified in `N0_SchemaCreation.py`, the notebook `N2_RebuildSchemas.py` creates empty dataframes for the bronze, silver and gold notebooks to use.

`N3_Bronze.py` notebook retrieves the files from this repository and saves them in `/tmp/Abdus/github`. It then extracts each zip file to `/tmp/Abdus/landing` before applying the related schema specified in `N0_SchemaCreation.py` and saving as delta to `/tmp/Abdus/Bronze`. Before moving on the Silver, the `github` and `landing` folders are deleted.

`N4_Silver.py` notebook loads the files created in the Bronze folder and applies the silver schemas specified in `N0_SchemaCreation.py`. It then saves these new dataframes to delta files in `/tmp/Abdus/Silver`

`N5_Gold.py` is the final layer for the Medallion Architecture used and contains the most transformations and manipulations towards the data. This is because we are applying the Star Schema and columns are moved around and new tables are created to match the design specified. This notebook creates 2 fact tables - `trips` and `payments` and 5 dimension tables - `riders`, `stations`, `bikes`, `dates` and `times`.

Now that the data has been transformed to suit business needs, it can be queried to answer business use cases. The following are questions that `N6_BusinessOutcomes.py` answers:

Q1) Analyse how much time is spent per ride:
* 1A) Based on date and time factors such as day of week
* 1B) Based on date and time factors such as time of day
* 1C) Based on which station is the starting station
* 1D) Based on which station is the ending station
* 1E) Based on age of the rider at time of the ride
* 1F) Based on whether the rider is a a member or a casual rider

Q2) Analyse how much money is spent:
* 2A) Per month
* 2B) Per quarter
* 2C) Per year
* 2D) Per member, based on the age of the rider at account start

Q3) EXTRA CREDIT - Analyse how much money is spent per member:
* 3A) Based on how many rides the rider averages per month
* 3B) Based on how many minutes the rider spends on a bike per month


The final notebook `N7_AutomatedTests.py` uses data from `N0_SchemaCreation.py` and `N6_BusinessOutcomes.py` to ensure that the data is correct and the business outcomes can be answered. It does this usinng `assert` statements. For example:
```python
assert gold_trips_df.schema == gold_trips_schema, "Schema mismatch on: Trips table"

#Q1C) timeSpentPerRide_StartStation
assert timeSpentPerRide_StartStation.count() == 74, "Incorrect number of rows in Q1C, expecting 74"
```

Finally, a workflow was created to automate notebooks `N0`-`N5` as theses notebookes are designed to run sequentially.

## Evidence
The following are images showcasing various aspects of the project such as directory layouts, snapshots of the dataframe tables and workflow results:

   <details>
   <summary>DBFS Directory layout for Bronze</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/dbfs_bronze.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>

   <details>
   <summary>DBFS Directory layout for Silver</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/dbfs_silver.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
  
   <details>
   <summary>DBFS Directory layout for Gold</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/dbfs_gold.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Trips Fact Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_trips.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Payments Fact Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_payments.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Riders Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_riders.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Stations Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_stations.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>
   
   <details>
      
   <summary>Bikes Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_bikes.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>

   <details>
      
   <summary>Dates Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_dates.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>

   <details>
      
   <summary>Times Dimension Table</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/df_times.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>
   >
   
   </details>

   <details>
      
   <summary>Workflow Results</summary>

   ><p align="center">
   ><img src="https://github.com/abdusqualyfi/StarSchemaProject/raw/main/img/workflow_run.png"
   >  alt="Size Limit comment in pull request about bundle size changes"
   ></p>

   </details>
