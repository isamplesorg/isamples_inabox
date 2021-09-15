# syntax=docker/dockerfile:1
FROM python:latest

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .
CMD [ "uvicorn", "isb_web.main:app", "--host", "0.0.0.0", "--port", "8000"]