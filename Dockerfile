FROM python:3.10-slim-bullseye
COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y\
 python3-pip && apt-get install vim -y

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install poetry

RUN apt-get -y install git