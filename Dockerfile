FROM        python:3.5
MAINTAINER  Michael B. Klein <michael.klein@northwestern.edu>
RUN         mkdir -p /var/app /data
WORKDIR     /var/app
ADD         . lakesuperior
WORKDIR     /var/app/lakesuperior
RUN         cp ./docker/etc/* ./etc.defaults/
RUN         pip install -r requirements.txt
CMD         ./docker/docker_entrypoint
EXPOSE      8000
HEALTHCHECK --interval=30s --timeout=5s \
  CMD curl -X OPTIONS -f http://localhost:8000/ || exit 1
