# Real Time data Analysis using kafka,pyspark, postreSQl and Grafana

## overview
Nowadays, companies are dealing with an increasing amount of data. Therefore, it is important to build efficient data pipelines to handle these large volumes of data. Sometimes, companies may need to have a real-time overview of their data for possible decision-making. The aim of this project is to demonstrate how you can handle streaming data to build insightful dashboards using Apache Spark for generating streaming data, Apache Kafka for processing them in real-time, PostgreSQL for saving this data, and Grafana for building insightful visualizations

## Data Architecture
![Github Logo](https://github.com/urbainze/Big-Data-Project/blob/main/i9.PNG)

## setting up the environement 
We'll be using a set of docker containers . more details are given below to help you setting up the conatainers .

## prerequisities
Ensure you have Docker installed . whether you are using Linux , mac or windows it's quite easy to get Docker installed on you machine .
if you don't know how to install Docker you can follow the steps in these links to do it . for [mac](https://docs.docker.com/desktop/install/mac-install/) , for [Linux](https://docs.docker.com/desktop/install/linux-install/) and this one for [Windows](https://docs.docker.com/desktop/install/windows-install/) 

## 1-launch containers
To launch the containers, make sure you're in the directory where the `Docker-compose.yml` is and then run :
`docker-compose up`.
To start the containers. You'll see the 6 containers starting and running .
## 2-create a topic 
To start a producer, kafka need a topic so the first thing we're going to do is to create a topic. To do so, just execute this command in a command prompt : 
`docker exec -it kafka kafka-topics --create --topic purchasedata --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092`
## 3-start a producer 
Before to start a producer you should first install kafka-python . so open your command prompt and type 
`pip install kafka-python` .
once it's done, open a command promt , go in the directory where the `purchase_data.py` is located and execute this commande :
`python purchase_data.py`
## 4-start processing the data 
To start processing the data sent by kafka to spark . you should enter in the spark container . in the docker-compose file of this project i called the container hoster the spark service spark .
so to get in the container execute : `dokcer exec -it bash`. once you're in the container start the processing by executing the following command :
`spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0 processing_data.py`
## 5-visualize your data .
You can access grafana by going in your web browser and typing : `localhost:3000` . you'll see and interface and the credentials are admin for the login and admin for the password
.after that you need to add postgresql as your data source .use the configurations set in the docker-compse.yml file for connecting the database to grafana .
once it's done you can build your dashboard


![render](https://github.com/urbainze/Big-Data-Project/blob/main/im1.PNG)
