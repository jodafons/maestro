ARG DOCKER_NAMESPACE
FROM ${DOCKER_NAMESPACE}/base-server
LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"
#USER root
ENV DOCKER_IMAGE="true"

SHELL [ "/bin/bash", "-c" ]
WORKDIR /app
COPY api api
COPY schedule.py . 
COPY enumerations.py .
COPY models.py .
COPY main.py .
EXPOSE 80
CMD ["uvicorn" , "main:app", "--host","0.0.0.0","--port", "80"]

