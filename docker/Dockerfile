FROM osgeo/gdal:ubuntu-small-latest
COPY requirements.txt requirements.txt

RUN apt-get update && apt-get install -y\
 python3-pip && apt-get install vim -y

RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get -y install git