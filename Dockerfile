FROM python:3.10
LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

RUN pip install -U poetry
WORKDIR /app

COPY ./pyproject.toml .
COPY ./poetry.lock .

RUN poetry config virtualenvs.create false
RUN poetry lock
RUN poetry install --no-dev --no-interaction --no-ansi --no-root

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9053"]


