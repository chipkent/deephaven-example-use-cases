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
g++ -dynamiclib -o "$OUTPUT_DIR/libblackscholes.${SUFFIX}" ./src/main/cpp/blackscholes.cpp

# Compile BlackScholesPreset
javac -cp javacpp.jar -d ${OUTPUT_DIR} src/main/java/io/deephaven/presets/BlackScholesPreset.java

### Generate BlackScholes.java and JNI Code
java -cp javacpp.jar:${OUTPUT_DIR} org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp  io.deephaven.presets.BlackScholesPreset -d ${OUTPUT_DIR}/src/main/java
javac -cp javacpp.jar:src/main/java -d ${OUTPUT_DIR} ${OUTPUT_DIR}/src/main/java/io/deephaven/BlackScholes.java
java -cp javacpp.jar:${OUTPUT_DIR}:src/main/java org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp -Dplatform.linkpath=${OUTPUT_DIR} io.deephaven.BlackScholes -d ${OUTPUT_DIR}

# Compile Everything
javac -cp javacpp.jar:src/main/java -d ${OUTPUT_DIR} `find src -name \*.java` `find ${OUTPUT_DIR}/src -name \*.java`

# Create a JAR file
jar cf ${OUTPUT_DIR}/blackscholes.jar -C ${OUTPUT_DIR}/ .

echo "Build successful!"

# Run the Application
echo "Running a test application..."
java -Djava.library.path=${OUTPUT_DIR} -cp ${OUTPUT_DIR}/blackscholes.jar:javacpp.jar org.example.Main
