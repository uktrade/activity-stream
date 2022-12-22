FROM python:3.8

EXPOSE 8083

RUN mkdir -p /app
WORKDIR /app

ADD requirements.txt .

RUN pip install -r requirements.txt

ADD . /app/

CMD ["sh", "start-dev.sh"]