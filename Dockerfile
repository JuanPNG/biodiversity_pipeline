FROM apache/beam_python3.12_sdk:2.64.0

# Set locale for I/O and encoding
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

# System dependencies for geo stack
RUN apt-get update && apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libspatialindex-dev \
    libsnappy1v5 \
    locales \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /app/requirements.txt

# Copying code and files
COPY . /app

# Install package
RUN pip install .
