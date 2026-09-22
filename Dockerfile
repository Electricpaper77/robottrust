FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir . && useradd --create-home robottrust
USER robottrust
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "robottrust.api:app", "--host", "0.0.0.0", "--port", "8000"]
