
FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update

RUN apt-get install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    g++

COPY ./build /build

RUN ln -s /build/javacpp /javacpp && ln -s /build/pybind11 /pybind11

RUN cd /build && ./build.sh

RUN echo "source /build/bin/activate" >> /run_example.sh && \
    echo 'deephaven server --extra-classpath "/build/venv/example/blackscholes.jar /build/venv/example/javacpp.jar" --jvm-args -Djava.library.path=/build/venv/example' >> /run_example.sh && \
    chmod a+x /run_example.sh

CMD /run_example.sh

