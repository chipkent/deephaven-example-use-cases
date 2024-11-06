#!/bin/bash
set -e

echo "removing build/"
rm -rf "build/"

# Detect OS
OS=$(uname -s)
case "$OS" in
    Linux*)     OS="linux";;
    Darwin*)    OS="macosx";;
    CYGWIN*)    OS="windows";;
    MINGW*)     OS="windows";;
    *)          OS="unknown";;
esac

# Detect architecture
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)     ARCH="x86_64";;
    arm64)      ARCH="arm64";;
    aarch64)    ARCH="arm64";;
    *)          ARCH="unknown";;
esac

# Combine OS and architecture
PLATFORM="${OS}-${ARCH}"

# Determine shared library suffix
case "$OS" in
    linux)      SUFFIX="so";;
    macosx)     SUFFIX="dylib";;
    windows)    SUFFIX="dll";;
    *)          SUFFIX="unknown";;
esac

echo "Detected shared library suffix: $SUFFIX"

echo "Detected platform: $PLATFORM"

OUTPUT_DIR="build/$PLATFORM"
mkdir -p "$OUTPUT_DIR"

# Build the Shared Library
g++ -dynamiclib -o "$OUTPUT_DIR/libmyhello.${SUFFIX}" ./src/main/cpp/myhello.cpp

g++ -dynamiclib -o "$OUTPUT_DIR/libblackscholes.${SUFFIX}" ./src/main/cpp/blackscholes.cpp

# Compile HelloPreset.java
javac -cp javacpp.jar -d build src/main/java/org/example/presets/HelloPreset.java

javac -cp javacpp.jar -d build src/main/java/io/deephaven/presets/BlackScholesPreset.java

### Generate Hello.java and JNI Code
java -cp javacpp.jar:build org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp  org.example.presets.HelloPreset -d build/src/main/java
javac -cp javacpp.jar:src/main/java -d build/ build/src/main/java/org/example/Hello.java
java -cp javacpp.jar:build:src/main/java org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp -Dplatform.linkpath=build/${PLATFORM} org.example.Hello -d build/${PLATFORM}

java -cp javacpp.jar:build org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp  io.deephaven.presets.BlackScholesPreset -d build/src/main/java
javac -cp javacpp.jar:src/main/java -d build/ build/src/main/java/io/deephaven/BlackScholes.java
java -cp javacpp.jar:build:src/main/java org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp -Dplatform.linkpath=build/${PLATFORM} io.deephaven.BlackScholes -d build/${PLATFORM}

# Compile Everything
javac -cp javacpp.jar:src/main/java -d build/ `find src -name \*.java` `find build/src -name \*.java`

echo "Build successful!"

# Run the Application
java -Djava.library.path=./build/${PLATFORM} -cp build:javacpp.jar org.example.Main