#!/bin/bash
#
# Build script for JavaCPP Black-Scholes integration
#
# This script builds a complete JavaCPP integration by:
# 1. Compiling the C++ Black-Scholes library from shared source
# 2. Using JavaCPP to generate Java wrapper classes and JNI code
# 3. Compiling all Java code and packaging into a JAR
# 4. Running a test application to verify the integration
#
# The script automatically detects the platform (OS and architecture) and
# configures the build accordingly for cross-platform support.
#
# Requirements:
#   - Java JDK (for javac, java, jar commands)
#   - g++ or compatible C++ compiler
#   - javacpp.jar (JavaCPP library)
#   - Shared Black-Scholes source in ../shared/blackscholes/
#
# Output:
#   - build/<platform>/libblackscholes.<ext> - Native C++ library
#   - build/<platform>/blackscholes.jar - Java wrapper + JNI code
#
set -e

# ============================================================================
# Check Required Tools
# ============================================================================
command -v java >/dev/null 2>&1 || { echo "Error: java not found. Please install Java JDK."; exit 1; }
command -v javac >/dev/null 2>&1 || { echo "Error: javac not found. Please install Java JDK."; exit 1; }
command -v g++ >/dev/null 2>&1 || { echo "Error: g++ not found. Please install a C++ compiler."; exit 1; }

echo "JAVACPP: removing build/"
rm -rf "build/"

# ============================================================================
# Platform Detection
# ============================================================================
# Detect the operating system and architecture to configure platform-specific
# build settings (library extensions, compiler flags, etc.)

# Detect OS (Linux, macOS, Windows)
OS=$(uname -s)
case "$OS" in
    Linux*)     OS="linux";;
    Darwin*)    OS="macosx";;
    CYGWIN*)    OS="windows";;
    MINGW*)     OS="windows";;
    *)          OS="unknown";;
esac

# Detect architecture (x86_64, arm64)
ARCH=$(uname -m)
case "$ARCH" in
    x86_64)     ARCH="x86_64";;
    arm64)      ARCH="arm64";;
    aarch64)    ARCH="arm64";;  # Linux ARM64
    *)          ARCH="unknown";;
esac

# Combine OS and architecture (e.g., "macosx-arm64", "linux-x86_64")
PLATFORM="${OS}-${ARCH}"

echo "JAVACPP: Detected platform: $PLATFORM"

# Determine shared library file extension for this platform
case "$OS" in
    linux)      SUFFIX="so";;      # Linux: .so
    macosx)     SUFFIX="dylib";;   # macOS: .dylib
    windows)    SUFFIX="dll";;     # Windows: .dll
    *)          SUFFIX="unknown";;
esac

echo "JAVACPP: Detected shared library suffix: $SUFFIX"

# Determine compiler options for creating shared libraries
case "$OS" in
    linux)      COMPILE_OPTS=-shared;;      # Linux: -shared
    macosx)     COMPILE_OPTS=-dynamiclib;;  # macOS: -dynamiclib
    windows)    COMPILE_OPTS=-shared;;      # Windows: -shared
    *)          COMPILE_OPTS=-shared;;
esac

echo "JAVACPP: Detected compiler opts: $COMPILE_OPTS"

OUTPUT_DIR="build/$PLATFORM"
mkdir -p "$OUTPUT_DIR"

# ============================================================================
# Step 1: Build Native C++ Library
# ============================================================================
# Compile the shared Black-Scholes C++ implementation from ../shared/blackscholes/
# This creates the native library (e.g., libblackscholes.dylib on macOS)
g++ ${COMPILE_OPTS} -o "$OUTPUT_DIR/libblackscholes.${SUFFIX}" ../shared/blackscholes/blackscholes.cpp

# ============================================================================
# Step 2: Compile JavaCPP Preset Configuration
# ============================================================================
# The preset tells JavaCPP which headers to parse and how to generate bindings
javac -cp javacpp.jar -d ${OUTPUT_DIR} src/main/java/io/deephaven/presets/BlackScholesPreset.java

# ============================================================================
# Step 3: Generate Java Wrapper Classes and JNI Code
# ============================================================================
# JavaCPP parses the C++ header and generates:
#   - BlackScholes.java (Java wrapper class)
#   - JNI bridge code to call native functions
# The -Dplatform.includepath tells JavaCPP where to find the C++ headers
java -cp javacpp.jar:${OUTPUT_DIR} org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=../shared/blackscholes  io.deephaven.presets.BlackScholesPreset -d ${OUTPUT_DIR}/src/main/java

# Compile the generated BlackScholes.java wrapper class
javac -cp javacpp.jar:src/main/java -d ${OUTPUT_DIR} ${OUTPUT_DIR}/src/main/java/io/deephaven/BlackScholes.java

# Generate and compile the JNI bridge code
# This creates the native JNI library that connects Java to C++
java -cp javacpp.jar:${OUTPUT_DIR}:src/main/java org.bytedeco.javacpp.tools.Builder -Dplatform.includepath=../shared/blackscholes -Dplatform.linkpath=${OUTPUT_DIR} io.deephaven.BlackScholes -d ${OUTPUT_DIR}

# ============================================================================
# Step 4: Compile All Java Source Files
# ============================================================================
# Compile both our source files and JavaCPP-generated files
javac -cp javacpp.jar:src/main/java -d ${OUTPUT_DIR} `find src -name \*.java` `find ${OUTPUT_DIR}/src -name \*.java`

# ============================================================================
# Step 5: Package Everything into a JAR
# ============================================================================
# Create a JAR containing all compiled classes and native libraries
jar cf ${OUTPUT_DIR}/blackscholes.jar -C ${OUTPUT_DIR}/ .

echo "JAVACPP: Build successful!"

# ============================================================================
# Step 6: Run Test Application
# ============================================================================
# Verify the integration works by running the test application
# -Djava.library.path tells Java where to find native libraries
echo "JAVACPP: Running a test application..."
java -Djava.library.path=${OUTPUT_DIR} -cp ${OUTPUT_DIR}/blackscholes.jar:javacpp.jar org.example.Main
