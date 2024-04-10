FROM python:3.11

EXPOSE 8082

RUN mkdir -p /app
WORKDIR /app

ADD requirements.txt .

RUN pip install -r requirements.txt

ADD . /app/

CMD ["python", "app.py"]
