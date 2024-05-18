FROM faasm.azurecr.io/faabric-base:0.15.0
ARG FAABRIC_VERSION

# Flag to say we're in a container
ENV FAABRIC_DOCKER="on"
SHELL ["/bin/bash", "-c"]

# Put the code in place
RUN rm -rf /code \
    && mkdir -p /code/faabric \
    && git clone \
        -b state \
        https://github.com/PancakeTY/faabric \
        /code/faabric \
    && cd /code/faabric \
    && ./bin/create_venv.sh \
    && source venv/bin/activate \
    && inv dev.cmake --build=Release \
    && inv dev.cc planner_server

ENTRYPOINT ["/code/faabric/bin/planner_entrypoint.sh"]
