FROM python:3.11-slim

RUN apt update && apt install -y \
    cmake \
    make \
    g++ \
    build-essential \
    wget

WORKDIR /app 
COPY . . 
RUN wget -O /app/.models/gte-large-Q4.gguf https://huggingface.co/ChristianAzinn/gte-large-gguf/resolve/main/gte-large.Q4_K_M.gguf?download=true
RUN pip install -r requirements.txt
RUN mkdir .models
ENV MODEL_PATH=./.models/gte-large-Q4.gguf

EXPOSE 8080
CMD ["python3", "app.py"]