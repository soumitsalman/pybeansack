FROM python:3.11-alpine

WORKDIR /app 
COPY . . 

RUN pip install -r requirements.txt

EXPOSE 8080

ENV FLASK_APP=server.py
ENV FLASK_RUN_HOST=0.0.0.0

CMD [ "flask", "run" , "-p", "8080"]