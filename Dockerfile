FROM        python:3.7-alpine
LABEL       "maintainer"="Michael B. Klein <michael.klein@northwestern.edu>" \
            "maintainer"="Stefano Cossu <scossu@getty.edu>"

RUN         apk add --no-cache build-base git
RUN         pip3 install cython==0.29.6 cymem

RUN         mkdir -p /data
WORKDIR     /usr/local/lsup/src
COPY        .git ./.git
COPY        ext ./ext
COPY        bin ./bin
COPY        lakesuperior ./lakesuperior
COPY        setup.py README.rst ./

RUN         git submodule update --init ext
RUN         pip install -e .
COPY        ./docker/etc ./lakesuperior/etc.defaults

# Clean up build-required packages & dirs.
RUN         apk del build-base git
RUN         rm -rf .git

RUN         [ -f /data/ldprs_store/data.mdb ] || \
                echo yes | lsup-admin bootstrap

VOLUME      /data

EXPOSE      8000

ENTRYPOINT  ["gunicorn", "-c", "python:lakesuperior.wsgi", \
            "lakesuperior.server:fcrepo"]
#ENTRYPOINT  ["./bin/fcrepo"]
