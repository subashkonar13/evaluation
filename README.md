# Evaluation
Creating Docker image for Spark and using it to submit jobs

## Description
The objective is to ingest the csv files and apply certain transformations in the form of ETL using docker container
The folder/file structure is as below:

![enter image description here](https://raw.githubusercontent.com/subashkonar13/evaluation/main/images/folder.jpg)
`input`-Input csv files
`output`- output parquet files
`docker-compose.yml` is optional in case if the container config is required and the job has to submitted from spark-submit from host to container spark

## Prerequisites
1. Docker Desktop.
2. Apache Parquet Viewer from [here](https://apps.microsoft.com/store/detail/apache-parquet-viewer/9PGB0M8Z4J2T?hl=en-us&gl=us) 

**Note**: I am using Windows 11 to perform the installations.

## Task to be performed:
3. Run `docker pull subashkonar13/evaluation:latest`
![enter image description here](https://raw.githubusercontent.com/subashkonar13/evaluation/main/images/pull.jpg)
4. Then Run `docker run  subashkonar13/evaluation:latest driver local:///opt/application/main.py`
5. Get the **container name** attached to the image by running command `docker ps -a --format="container:{{.ID}} image:{{.Image}}"`. 
![enter image description here](https://raw.githubusercontent.com/subashkonar13/evaluation/main/images/run.jpg)
Since I am using windows OS,I would need to explicitly copy the files to host. In case of linux, the drive from linux host can be mounted easily to docker container path and files can be viewed from the host.
7. To copy the files to current host run (get the container name attached image from previous command) `docker cp <container name>:/opt/application/output C:/HD/Lecturio_ver4` 
8. Once the files  are copied,you can open the file in `Apache Parquet viewer`
