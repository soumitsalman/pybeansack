FROM python:3.11-alpine

WORKDIR /app 
COPY . . 

RUN pip install -r requirements.txt

EXPOSE 8080

ENV FLASK_APP=server.py
ENV FLASK_RUN_HOST=0.0.0.0

ENV ESPRESSO_APP_NAME "Espresso by Cafecit.io"
ENV COFFEEMAKER_BASE_URL https://beansackservice.purplesea-08c513a7.eastus.azurecontainerapps.io
ENV EMBEDDER_BASE_URL https://embeddings-service.purplesea-08c513a7.eastus.azurecontainerapps.io/embed

CMD [ "flask", "run" , "-p", "8080"]