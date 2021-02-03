# Production-Data-Pipeline-using-Apache-Airflow
Orchestrating data pipeline using advanced functionalities of Apache Airflow

## Introduction
In this project we want to use functiionalities of Apache Airflow to introduce more automation and monitoring to the Data Warehouse ETL pipelines. 
In this project,  we will be working with two datasets located in AWS S3:

Log data: s3://udacity-dend/log_data
Song data: s3://udacity-dend/song_data

These files have been saved in JSON format. Through this project we will be building Airflow DAGs to load the data from S3 to AWS Redshift and further build 
fact and dimension tables on the cluster to carry out data transformation.

## Prerequisite Installation

* Before setting up and running Apache Airflow, please install Docker and Docker Compose.
* Create file docker-compose.yml find its content in this [link](https://towardsdatascience.com/apache-airflow-and-postgresql-with-docker-and-docker-compose-5651766dfa96).
* Positioning in the root directory and execute: “docker-compose up” in the command prompt.It should make airflow accessible on localhost:8080.

## Launching Redshift Cluster

* Go to AWS Redshift Console
* Click Create cluster
* Give cluster settings:
        * Add VDC security group for redshift
        * Enable Public accessibility
<img src="images/redshift_clus.png" alt="drawing" width="800" height="300"/>

## Connect to Airflow

1. Click on the Admin tab and select Connections.
![Admin tab](https://video.udacity-data.com/topher/2019/February/5c5aaca1_admin-connections/admin-connections.png)

2. Under Connections, select Create.

3. Create connection for AWS hook, enter the following values:
* Conn Id: aws_credentials.
* Conn Type: Amazon Web Services.
* Login: Enter your Access key ID from the IAM User credentials.
* Password: Enter your Secret access key from the IAM User credentials.
<img src="images/aws_creds.png" alt="drawing" width="800" height="300"/>

Once you've entered these values, select Save and Add Another.

4. Craete connection for Redshift , enter the following values:
* Conn Id:  redshift.
* Conn Type:  Postgres.
* Host: Enter the endpoint of your Redshift cluster, excluding the port at the end.
* Schema:  This is the Redshift database you want to connect to (given at time of cluster creation).
* Login: Enter awsuser.
* Password: Enter the password you created when launching your Redshift cluster.
* Port: Enter 5439.
<img src="images/redshift.png" alt="drawing" width="800" height="300"/>

Once you've entered these values, select Save.

5. After seting up these connections. Go to homepage of Airflow UI, swith on your DAG button to start the DAG execution.

## DAG Architecture

There are four important component of our architecture.
1. MyDag.py contains all imports as well as declaratio of tasks that our dag comprises.
<img src="images/airflow_dag.png" alt="drawing" width="800" height="300"/>

2. subdag.py contains the subdags whcich are included in the dag.
<img src="images/subdag_1.png" alt="drawing" width="800" height="300"/>
<img src="images/subdag_2.png" alt="drawing" width="800" height="300"/>

3. Operators folder with operator templates for different operations executed in the dag.
4. Helper folder contains SQL transformation queries which we run during the DAG execution.

Add `default parameters` to the Dag template as follows:
* Dag does not have dependencies on past runs
* On failure, tasks are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Do not email on retry

Following are the four operators that I have included in the code:

1. StageToRedshiftOperator

* Load any JSON formatted files from S3 to Amazon Redshift.
* The operator creates and runs a SQL COPY statement based on the parameters provided.
* Load timestamped files from S3 based on the execution time and run backfills.

2.LoadDimensionOperator

* To load data from staging layer to dimension layer.
* Use SQL queries defined in helper folder to execute this operator.
* This operator is expected to take as input a SQL statement and target database on which to run the query against.
* Dimension loads will be done in truncate-insert pattern where the target table is emptied before the load.

3.LoadFactOperator

* To load data from staging layer to fact layer.
* Use SQL queries defined in helper folder to execute this operator.
* This operator is expected to take as input a SQL statement and target database on which to run the query against.

4. DataQualityOperator

* Used to check duplicate records in the dimension anf fact tables.
* The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests.
* Test result and expected results are checked and if there is no match, operator should raise an exception and the task should retry and fail eventually

   
## Start the DAG
Start the DAG by switching it state from OFF to ON.
<img src="images/home.png" alt="drawing" width="800" height="300"/>

Refresh the page and click on the Airflow_Project to view the current state.
<img src="images/dag_overview.png" alt="drawing" width="800" height="300"/>



