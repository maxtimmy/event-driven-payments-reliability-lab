FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml setup.cfg ./
COPY src ./src
COPY alembic.ini ./
COPY migrations ./migrations
RUN pip install --no-cache-dir .
CMD ["uvicorn", "payments_lab.api:app", "--host", "0.0.0.0", "--port", "8000"]
