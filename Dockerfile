FROM ubuntu:20.04

ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt-get update && apt-get install -y \
	python3 \
	python3-gdal \
	python3-pip

# Use /code (convention) as the base directory. Install the requirements in
# there.
WORKDIR /code
COPY requirements.txt ./
RUN pip3 install --no-cache-dir -r requirements.txt