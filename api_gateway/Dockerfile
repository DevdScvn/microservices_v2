FROM python:3.12.9-bookworm

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

RUN pip install --upgrade pip wheel "poetry==2.0.1"

RUN poetry config virtualenvs.create false

COPY pyproject.toml poetry.lock ./

RUN poetry install

COPY src .

RUN chmod +x prestart.sh

ENTRYPOINT ["./prestart.sh"]

CMD ["python", "main.py"]