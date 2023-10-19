# Reference used: https://github.com/docker/awesome-compose/blob/master/flask/app/Dockerfile

FROM python:3.9

WORKDIR /webserver

COPY requirements.txt .

RUN pip install --trusted-host pypi.python.org -r requirements.txt

COPY . .

ENV PORT=8080
EXPOSE 8080

ENTRYPOINT ["python3"]
CMD ["-u", "distributed_kvs.py"]