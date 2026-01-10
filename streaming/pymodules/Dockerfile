FROM apache/spark:3.5.6

USER root

COPY requirements.txt /opt/spark/requirements.txt

WORKDIR /opt/spark

RUN pip install -U pip setuptools
RUN pip install --upgrade pip -r /opt/spark/requirements.txt

USER spark
