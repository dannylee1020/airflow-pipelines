# Data Pipelines with Airflow
## Overview
This is a practice project for running airflow to automate ETL jobs, data pipelines and integrating other tools such as dbt to the analytics workflow.

## Running dbt with Airflow
There are several approaches to run dbt with airflow to schedule jobs and orchestrate analytics workflow. The official dbt documentation recommends three ways to run dbt on airflow. 

1. Using the [dbt-cloud-plugin](https://github.com/dwallace0723/dbt-cloud-plugin/)
2. Invoking dbt through the `BashOperator` 
3. Installing the [airflow-dbt](https://pypi.org/project/airflow-dbt/) python package

I chose the 2nd aprroach and came up with another approach.  

### Using BashOperator
`BashOperator` in airflow simply executes a shell command. Because the primary dbt interface is with command line, it is pretty useful to run different dbt tasks once you have the dbt models ready. 
<br>
This approach, however, is not very good for scaling. If the number of dbt models increase, it would be very difficult to manage and debug. 


### Using Docker 
Another approach is to containerize dbt projects and use `DockerOperator` to run the dbt inside the docker container. When scale grows, multiple docker containers can be managed by Kubernetes. 


## Reference
[Running dbt in production](https://docs.getdbt.com/docs/running-a-dbt-project/running-dbt-in-production/)
<br>
[Creating a Docker image for a dbt project](https://github.com/dannylee1020/dbt-docker)
