FROM python:3.12-slim

# Copy Apache Beam SDK launcher and libraries from official base images
COPY --from=apache/beam_python3.12_sdk:2.64.0 /opt/apache/beam /opt/apache/beam
COPY --from=gcr.io/dataflow-templates-base/python312-template-launcher-base:latest /opt/google/dataflow/python_template_launcher /opt/google/dataflow/python_template_launcher

# Copy all source files
# COPY . /app
# WORKDIR /app
# ENV PYTHONPATH=/app

ARG WORKDIR=/template
WORKDIR ${WORKDIR}

COPY flex_templates flex_templates
COPY requirements.txt .
COPY setup.py .
COPY src src
COPY utils utils

# Locale for consistent UTF-8 support
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# Set required Flex Template ENV variables
ENV FLEX_TEMPLATE_PYTHON_PY_FILE=${WORKDIR}/flex_templates/launch_flex_template.py
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=${WORKDIR}/requirements.txt

# Install geo/raster system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    libsnappy1v5 \
    locales \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONPATH="${WORKDIR}"

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
# RUN pip install pyarrow==15.0.2 google-cloud-bigquery==3.17.2
RUN pip install -e .

# Verify installation before launching anything
RUN python -c "import utils.helpers, src.taxonomy_pipeline; print('Package structure validated')"

