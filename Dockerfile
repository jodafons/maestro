FROM python:3.10
LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"

RUN pip install -U poetry
WORKDIR /app

COPY maestro maestro 
COPY scripts scripts
COPY ./pyproject.toml .
COPY ./poetry.lock .
RUN poetry config virtualenvs.create false
RUN poetry lock
RUN poetry install --no-dev --no-interaction --no-ansi --no-root
RUN ls -lisah maestro
ENV PYTHONPATH=${PYTHONPATH}:/app/maestro
ENV PATH=${PATH}:/app/scripts
ENV MAESTRO_PATH=/app
ENV MAESTRO_DOCKER_TEST="true"



