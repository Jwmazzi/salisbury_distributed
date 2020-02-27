yum install wget -y

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -P /opt
bash /opt/Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda3

/opt/miniconda3/bin/conda create --name worker python=3.6 -y
/opt/miniconda3/envs/worker/bin/pip install newspaper3k boto3 celery pandas awscliyum install wget -y

wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -P /opt
bash /opt/Miniconda3-latest-Linux-x86_64.sh -b -p /opt/miniconda3

/opt/miniconda3/bin/conda create --name worker python=3.6 -y
/opt/miniconda3/envs/worker/bin/pip install newspaper3k boto3 celery pandas awscli
