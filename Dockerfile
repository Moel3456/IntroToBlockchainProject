FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

COPY src/ ./src/

RUN mkdir -p /app/data

EXPOSE 18018

CMD ["python", "-m", "src.main"]