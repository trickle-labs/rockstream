FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir sqlalchemy alembic psycopg2-binary "psycopg[binary]"
COPY . .
CMD ["python", "app.py"]
