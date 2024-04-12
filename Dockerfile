FROM python:3.10

EXPOSE 8081

RUN mkdir -p /app
WORKDIR /app

ADD ./core/requirements.txt .

RUN pip install -r requirements.txt

ADD . /app/

CMD ["sh", "start-dev.sh"]
