#!/bin/bash
# update and install some useful yum packages

sudo yum -y install wget tar gzip gcc make expect

sudo python3 -m pip install --upgrade pip
sudo python3 -m pip install configparser
sudo python3 -m pip install "boto3>=1.15.0,<1.18.0"
sudo python3 -m pip install pandas --target='/usr/lib/python3.6/dist-packages'
sudo python3 -m pip uninstall "numpy==1.14.5" --yes
sudo python3 -m pip uninstall "numpy==1.14.5" --yes
sudo python3 -m pip uninstall "numpy==1.14.5" --yes
sudo python3 -m pip install "numpy==1.18.1" --target='/usr/lib/python3.6/dist-packages'
sudo python3 -m pip install fuzzywuzzy
sudo python3 -m pip install python-Levenshtein
#sudo python3.6 -m pip uninstall "numpy==1.14.5" --yes
#sudo python3 -m pip install "numpy==1.18.1" --target='/usr/local/lib64/python3.6/site-packages'
#sudo mv /usr/local/lib64/python3.6/site-packages/numpy /usr/lib/python3.6/dist-packages/numpy
