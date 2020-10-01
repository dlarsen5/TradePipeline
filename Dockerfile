FROM ubuntu:16.04
FROM python:latest

WORKDIR /
RUN mkdir app

COPY ./requirements.txt /app/
RUN pip install -r /app/requirements.txt && rm /app/requirements.txt

COPY ./py/ /app/

CMD ["python", "/app/agent.py"]