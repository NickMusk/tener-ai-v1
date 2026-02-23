FROM python:3.11-slim

WORKDIR /app

COPY . /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    TENER_HOST=0.0.0.0 \
    TENER_PORT=8080

EXPOSE 8080

CMD ["python", "-m", "tener_ai"]
