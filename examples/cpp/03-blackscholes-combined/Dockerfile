# Black-Scholes Combined Example - Docker Image
FROM ubuntu:24.04

# Python version to install (can be overridden: docker build --build-arg PYTHON_VERSION=3)
# Note: Use "3" for default python3, or "3.12" for specific version via deadsnakes PPA
ARG PYTHON_VERSION=3.12

# Install system dependencies
# If using a specific Python version (e.g., 3.12), add deadsnakes PPA
# Work around Ubuntu 24.04 ARM64 GPG key deprecation issues
RUN apt-get update --allow-insecure-repositories || apt-get update && \
    apt-get install -y --allow-unauthenticated software-properties-common gnupg && \
    echo 'APT::Key::Assert-Pubkey-Algo ">=rsa2048,>=dsa1024,>=ecc256";' > /etc/apt/apt.conf.d/99PubKeyAlgoWorkaround && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update

RUN apt-get install -y \
    python${PYTHON_VERSION} \
    python${PYTHON_VERSION}-venv \
    python${PYTHON_VERSION}-dev \
    g++ \
    openjdk-17-jdk

# Copy the C++ integration examples
COPY shared /cpp-examples/shared
COPY 01-blackscholes-javacpp /cpp-examples/01-blackscholes-javacpp
COPY 02-blackscholes-pybind11 /cpp-examples/02-blackscholes-pybind11
COPY 03-blackscholes-combined/build.sh /cpp-examples/03-blackscholes-combined/build.sh
COPY 03-blackscholes-combined/examples /cpp-examples/03-blackscholes-combined/examples

WORKDIR /cpp-examples/03-blackscholes-combined

# Build both integrations and set up the environment
# Set PYTHON to match the installed version
ENV PYTHON=python${PYTHON_VERSION}
RUN ./build.sh

# Create startup script
RUN echo "source /cpp-examples/03-blackscholes-combined/venv/bin/activate" > /run_example.sh && \
    echo 'cd /cpp-examples/03-blackscholes-combined' >> /run_example.sh && \
    echo 'deephaven server --extra-classpath "./venv/example/blackscholes.jar ./venv/example/javacpp.jar" --jvm-args "-Djava.library.path=./venv/example -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"' >> /run_example.sh && \
    chmod a+x /run_example.sh

CMD bash -c "/run_example.sh"

