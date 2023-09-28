FROM bitnami/python:3.7.16

COPY requirements.txt requirements.txt
RUN apt-get update &&  apt-get install software-properties-common -y --no-install-recommends
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install  -y --no-install-recommends  --reinstall python3-distutils  \
    python3-dev gcc musl-dev
RUN apt-get install -y --no-install-recommends libpq-dev
RUN pip3 install -r requirements.txt

CMD [ "/bin/bash"]
