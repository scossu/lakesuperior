FROM        python:3.6
MAINTAINER  Michael B. Klein <michael.klein@northwestern.edu>
RUN         mkdir -p /usr/local /data
WORKDIR     /usr/local
ADD         . lakesuperior
WORKDIR     /usr/local/lakesuperior
RUN         pip install -e .
RUN         cp ./docker/etc/* ./lakesuperior/etc.defaults/
CMD         ./docker/docker_entrypoint
EXPOSE      8000
HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl -X OPTIONS -f http://localhost:8000/ || exit 1
