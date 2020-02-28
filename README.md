# Getting Started on the Processing Components

There are 3 components in our distributed processing system: a producer that sends work to a queue, a messaging broker
that will manage that queue, and consumers that will take work from the queue. 

# Installing RabbitMQ

Execute the following steps on your CentOS machine to get RabbitMQ running as our broker. 
* yum install -y git
* git clone https://github.com/Jwmazzi/salisbury_distributed.git /opt/geoanalytics
* /opt/geoanalytics/scripts/rabbitmq.sh

After you have completed these steps, the management interface to RabbitMQ should be available at 
http://\<_ip or fqdn_\>:15672

# Getting the Producer & Consumer Running

Both the producer and consumer roles can be built with the following steps. 

* yum install -y git
* git clone https://github.com/Jwmazzi/salisbury_distributed.git /opt/geoanalytics
* /opt/geoanalytics/scripts/python.sh

The only difference between the producer and consumer environments is how they will be daemonized in systemd. Go to the
services directory in /opt/geoanalytics and use the following steps to configure the appropriate service. 

* cp /opt/geoanalytics/services/geo-consumer.service /etc/systemd/system
* systemctl daemon-reload
* service geo-consumer start
* service geo-consumer status