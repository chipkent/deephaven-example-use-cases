#!/bin/bash
set -e

# Build the Shared Library

echo "removing build/"
rm -rf "build/"

# hardcode mac for now
PLATFORM="macosx-arm64"
OUTPUT_DIR="build/$PLATFORM"

mkdir -p "$OUTPUT_DIR"

g++ -dynamiclib -o "$OUTPUT_DIR/libmyhello.dylib" ./src/main/cpp/myhello.cpp

# Compile HelloPreset.java
javac -cp javacpp.jar -d build src/main/java/org/example/presets/HelloPreset.java

# Generate hello.java and JNI Code
java -cp javacpp.jar:build org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp  org.example.presets.HelloPreset -d src/main/java
javac -cp javacpp.jar:src/main/java -d build/ src/main/java/org/example/hello.java
java -cp javacpp.jar:build:src/main/java org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=src/main/cpp -Dplatform.linkpath=build/macosx-arm64 org.example.hello -d build/macosx-arm64

# Compile Everything
javac -cp javacpp.jar:src/main/java -d build/ src/main/java/org/example/*.java

# Run the Application
java -Djava.library.path=./build/macosx-arm64/ -cp build:javacpp.jar org.example.Main