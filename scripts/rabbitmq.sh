yum install wget -y

wget https://github.com/rabbitmq/erlang-rpm/releases/download/v22.2.6/erlang-22.2.6-1.el6.x86_64.rpm -P /opt
yum install /opt/erlang-22.2.6-1.el6.x86_64.rpm -y

wget https://github.com/rabbitmq/rabbitmq-server/releases/download/v3.8.2/rabbitmq-server-3.8.2-1.el7.noarch.rpm -P /opt
yum install /opt/rabbitmq-server-3.8.2-1.el7.noarch.rpm -y

chkconfig rabbitmq-server on

service rabbitmq-server enable
service rabbitmq-server start

rabbitmq-plugins enable rabbitmq_management

rabbitmqctl add_user siteadmin gis12345
rabbitmqctl set_user_tags siteadmin administrator
rabbitmqctl set_permissions -p / siteadmin ".*" ".*" ".*"

