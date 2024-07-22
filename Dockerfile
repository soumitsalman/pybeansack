FROM python:3.11-alpine

RUN apk update && apk add --no-cache \
    cmake \
    make \
    g++ \
    build-base

WORKDIR /app 
COPY . . 

RUN pip install -r requirements.txt

EXPOSE 8080

CMD ["python3", "app.py"]