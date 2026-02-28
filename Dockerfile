FROM python:3.11-slim
RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*
RUN pip install boto3
WORKDIR /app
COPY recorder.py .
CMD ["python", "-u", "recorder.py"]
