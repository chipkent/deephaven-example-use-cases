# Black-Scholes Combined Example - Docker Image
FROM ubuntu:24.04

# Install system dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update

RUN apt-get install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    g++ \
    openjdk-17-jdk

# Copy the C++ integration examples
COPY ../01-blackscholes-javacpp /cpp-examples/01-blackscholes-javacpp
COPY ../02-blackscholes-pybind11 /cpp-examples/02-blackscholes-pybind11
COPY ../scripts /cpp-examples/03-blackscholes-combined/scripts
COPY ../examples /cpp-examples/03-blackscholes-combined/examples

WORKDIR /cpp-examples/03-blackscholes-combined

# Build both integrations and set up the environment
RUN ./scripts/build_all.sh

# Create startup script
RUN echo "source /cpp-examples/03-blackscholes-combined/venv/bin/activate" > /run_example.sh && \
    echo 'cd /cpp-examples/03-blackscholes-combined' >> /run_example.sh && \
    echo 'deephaven server --extra-classpath "./venv/example/blackscholes.jar ./venv/example/javacpp.jar" --jvm-args "-Djava.library.path=./venv/example -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"' >> /run_example.sh && \
    chmod a+x /run_example.sh

CMD bash -c "/run_example.sh"

