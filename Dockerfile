FROM registry.access.redhat.com/ubi8/python-38:latest

ARG PIPENV_DEV=False

ENV LC_ALL=en_US.UTF-8 \
    LANG=en_US.UTF-8 \
    PIP_NO_CACHE_DIR=off \
    ENABLE_PIPENV=true \
    DISABLE_MIGRATE=true \
    DJANGO_READ_DOT_ENV_FILE=false

ENV SUMMARY="Parquet-compactor compresses multiple small parquet files in S3 into fewer files" \
    DESCRIPTION="Parquet-compactor compresses multiple small parquet files in S3 into fewer files"

LABEL summary="$SUMMARY" \
    description="$DESCRIPTION" \
    io.k8s.description="$DESCRIPTION" \
    io.k8s.display-name="Parquet-compactor" \
    io.openshift.tags="builder,python,python38,rh-python38" \
    com.redhat.component="python38-docker" \
    name="Parquet-compactor" \
    version="1" \
    maintainer="Red Hat Cost Management Services"

USER root

# Copy application files to the image.
COPY . /tmp/src/.


RUN /usr/bin/fix-permissions /tmp/src && \
chmod 755 $STI_SCRIPTS_PATH/assemble $STI_SCRIPTS_PATH/run

RUN groupadd -g 1000 koku \
    && useradd -m -s /bin/bash -g 1000 -u 1000 -G root koku \
    && chmod g+rwx /opt

USER 1000

EXPOSE 8080

RUN $STI_SCRIPTS_PATH/assemble

# Set the default CMD
CMD $STI_SCRIPTS_PATH/run
