FROM python:3.11-slim

RUN apt-get update && apt-get install -y wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app 
COPY . . 

RUN mkdir models
RUN wget -O models/nomic.gguf "https://huggingface.co/nomic-ai/nomic-embed-text-v1.5-GGUF/resolve/main/nomic-embed-text-v1.5.Q8_0.gguf?download=true"
RUN pip install -r requirements.txt

EXPOSE 8080

CMD ["python3", "app.py"]