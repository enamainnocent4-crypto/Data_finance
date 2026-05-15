FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install kafka-python

CMD ["python", "transactions_producer.py"]