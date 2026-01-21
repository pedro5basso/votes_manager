FROM apache/spark:3.5.6

USER root

COPY requirements.txt /opt/spark/requirements.txt
COPY votes.csv /opt/tmp/votes.csv

WORKDIR /opt/spark

RUN pip install -U pip setuptools
RUN pip install --upgrade pip -r /opt/spark/requirements.txt

USER spark
