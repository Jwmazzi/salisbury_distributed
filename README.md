# Overview

This repository contains a number of files to support a lecture around distributed data processing with Python using
Celery and RabbitMQ. Before getting started on the processing piece, you will need to build the data set that is 
processed and explored below.

# Collecting the Sample Data

Follow the steps below to extract GDELT data for 2018 and get it uploaded into S3. 

* Install Git on your machine and then clone this repository into your working directory. 
* TODO - Provide YML File for Windows  & Linux to Build Environment
* Run extraction.py
* Use awscli to push to s3 (e.g. aws s3 cp gdelt_events s3://bucket-name --recursive)
  * You can also work with the extracted data locally if you do not have access to AWS. All of the code assumes that
  s3 is part of your solution, but the changes to work locally should be minimal.

# Getting Started on the Processing Components

There are 3 components in our distributed processing system: a producer that sends work to a queue, a messaging broker
that will manage communication with the queue, and consumers that will take work from the queue. This particular lecture
assumes that everything will be done on CentOS 7, but everything should work on RHEL and (with a few corrections to the bash scripts) 
maybe also on other Linux distributions.

# Installing RabbitMQ

Execute the following steps on your CentOS machine to get RabbitMQ running as our broker. 
* yum install -y git
* git clone https://github.com/Jwmazzi/salisbury_distributed.git /opt/geoanalytics
* chmod +x /opt/geoanalytics/scripts/rabbitmq.sh
* /opt/geoanalytics/scripts/rabbitmq.sh

After you have completed these steps, the management interface to RabbitMQ should be available at 
http://\<_ip or fqdn_\>:15672

# Getting the Producer & Consumer Running

Both the producer and consumer roles can be built with the following steps. 

* yum install -y git
* git clone https://github.com/Jwmazzi/salisbury_distributed.git /opt/geoanalytics
* chmod +x /opt/geoanalytics/scripts/python.sh
* /opt/geoanalytics/scripts/python.sh

The only difference between the producer and consumer environments is how they will be daemonized via systemd. Go to the
services directory in /opt/geoanalytics and use the following steps to configure the appropriate service. 

* source /opt/miniconda3/bin/activate worker
* aws configure
  * Input Access Keys for S3 Bucket
* cp /opt/geoanalytics/services/geo-consumer.service /etc/systemd/system
* systemctl daemon-reload
* service geo-consumer start
* service geo-consumer status