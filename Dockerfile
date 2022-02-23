FROM ubuntu:focal

ARG SPARK_VER=3.0.1
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && ln -s /usr/bin/python3 /usr/local/bin/python3 \
  && ln -s /usr/bin/python3 /usr/local/bin/python \
  && pip3 install --upgrade pip

RUN apt install -y openjdk-11-jre-headless

RUN pip install pyspark
RUN pip install jupyterlab

RUN jupyter lab --generate-config \
  && echo "c.NotebookApp.token = ''" >> /root/.jupyter/jupyter_lab_config.py \
  && echo "c.NotebookApp.password = ''" >> /root/.jupyter/jupyter_lab_config.py

WORKDIR /opt/workspace

CMD jupyter lab --ip=0.0.0.0 --no-browser --allow-root
