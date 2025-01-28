FROM python:3.11-slim

RUN apt update && apt install -y \
    cmake \
    make \
    g++ \
    build-essential \
    wget


WORKDIR /espresso
COPY . . 

RUN mkdir ./.models
RUN wget -O ./.models/gte-large-Q4.gguf https://huggingface.co/ChristianAzinn/gte-large-gguf/resolve/main/gte-large.Q4_K_M.gguf
ENV EMBEDDER_MODEL=/espresso/.models/gte-large-Q4.gguf
ENV EMBEDDER_N_CTX=512

RUN pip install -r requirements.txt
RUN pip install -r app/pybeansack/requirements.txt

EXPOSE 8080
CMD ["python3", "run.py"]
