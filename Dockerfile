FROM python:3.12

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app
COPY storage ./storage

EXPOSE 8000

CMD ["sh", "-c", "python -c \"from app.core.database import init_db; init_db()\" && exec gunicorn app.main:app -k uvicorn.workers.UvicornWorker -w ${GUNICORN_WORKERS:-2} -b 0.0.0.0:8000 --access-logfile - --error-logfile -"]
