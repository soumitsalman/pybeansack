FROM python:3.11-alpine

WORKDIR /app 
COPY . . 

RUN pip install -r requirements.txt

EXPOSE 8080

ENV FLASK_APP=server.py
ENV FLASK_RUN_HOST=0.0.0.0

ENV ESPRESSO_APP_NAME "Espresso by Cafecit.io"
ENV BEANSACK_URL "https://beansackservice.purplesea-08c513a7.eastus.azurecontainerapps.io"
ENV REDDITOR_URL "https://redditcollector.orangeflower-f8e1f6b0.eastus.azurecontainerapps.io"

CMD [ "flask", "run" , "-p", "8080"]