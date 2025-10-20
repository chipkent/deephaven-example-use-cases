
FROM ubuntu:24.04

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

COPY ./build /build

RUN ln -s /build/javacpp /javacpp && ln -s /build/pybind11 /pybind11

RUN cd /build && ./build.sh

RUN echo "source /build/venv/bin/activate" > /run_example.sh && \
    echo 'deephaven server --extra-classpath "/build/venv/example/blackscholes.jar /build/venv/example/javacpp.jar" --jvm-args "-Djava.library.path=/build/venv/example -DAuthHandlers=io.deephaven.auth.AnonymousAuthenticationHandler"' >> /run_example.sh && \
    chmod a+x /run_example.sh

CMD bash -C "/run_example.sh"

