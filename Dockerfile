FROM tensorflow/tensorflow:2.11.0-gpu
LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"
USER root
SHELL [ "/bin/bash", "-c" ]

RUN pip install --upgrade pip
RUN pip install virtualenv

RUN mkdir /app
WORKDIR /app

COPY orchestra /app/orchestra
COPY scripts /app/scripts
COPY setup.py .
COPY requirements.txt .
COPY README.md .
RUN pip install .


