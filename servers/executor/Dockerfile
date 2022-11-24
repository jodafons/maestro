FROM jodafons/orchestra:base
LABEL maintainer "Joao Victor da Fonseca Pinto <jodafons@lps.ufrj.br>"
USER root
SHELL [ "/bin/bash", "-c" ]
WORKDIR /app
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]

