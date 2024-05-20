FROM python:3.10.6
MAINTAINER Elshan Chalabiyev
COPY app.py requirements.txt utils.py .
RUN apt update; apt upgrade -y
RUN pip install --upgrade pip
RUN pip install -r requirements.txt
ENTRYPOINT ["python", "app.py"]