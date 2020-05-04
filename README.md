# DataEngineer
The project contains solution to process following csv data files in Spark and extract meaningful information for 
analysis. The generated data is stored in parquet format on the local file system.

1. users.csv 
2. movies.csv 
3. ratings.csv 

**Local development environment:**
* Java 1.8.0_251
* Scala 2.11.12
* Spark 2.4.5
* sbt 1.3.10

**Pre-condition to run:**

* Clone project locally and cd to the project directory

* Test data must exist in the /tmp folder in the root folder of local file system

**Command to run:** 

**Run** 'sbt package' to compile and package jar

**Run** 'spark-submit --class DataEnricher --master "local[4]" target/scala-2.11/dataengineer_2.11-0.1.jar' 
to run Spark job locally

**Check logs and search for 'Old and new dataframes written to' to find parquet files output folder.**
```
Ex: Old and new dataframes written to /Users/rajeevsachdeva/tmp/nw/output/477b2360-d0ca-4eac-a573-83d8d9041bb9 
in parquet format
```

**Out of scope:**

Unit testing

**Additional notes**

The project is intended to show general Spark and Scala knowledge to process data files locally, therefore 
not enhanced to produce full featured application. The scope of work is limited to requirements defined in the exercise.
