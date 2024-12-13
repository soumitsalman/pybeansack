FROM python:3.11-slim

RUN apt update && apt install -y \
    cmake \
    make \
    g++ \
    build-essential \
    wget

COPY . . 
RUN pip install -r requirements.txt
RUN pip install -r app/pybeansack/requirements.txt

ENV LLM_BASE_URL=https://api.deepinfra.com/v1/openai
ENV EMBEDDER_MODEL=thenlper/gte-large
ENV EMBEDDER_N_CTX=496

ENV APP_NAME=Espresso
ENV OTEL_SERVICE_NAME=ESPRESSO-WEB

EXPOSE 8080
CMD ["python3", "run.py"]
