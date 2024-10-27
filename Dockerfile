FROM python:3.11-slim

RUN apt update && apt install -y \
    cmake \
    make \
    g++ \
    build-essential \
    wget

WORKDIR /app 
COPY . . 
RUN pip install -r requirements.txt

ENV APP_NAME="Espresso by Project Cafecito"
ENV OTEL_SERVICE_NAME=ESPRESSO-WEB

EXPOSE 8080
CMD ["python3", "app.py"]