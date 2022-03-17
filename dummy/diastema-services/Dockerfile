FROM python:3.8

RUN apt update -y
RUN apt upgrade -y

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

CMD ["gunicorn", "-b", "0.0.0.0:5000", "app:app"]