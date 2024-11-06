# JavaCpp Example 

This is a simple example using [JavaCpp](https://github.com/bytedeco/javacpp) to call a C++ function from Java.
[JavaCpp](https://github.com/bytedeco/javacpp) is a tool that makes it easy to wrap C++ code in Java.

For more details on [JavaCpp](https://github.com/bytedeco/javacpp) see [https://github.com/bytedeco/javacpp](https://github.com/bytedeco/javacpp).

For details on integrating [JavaCpp](https://github.com/bytedeco/javacpp) into a build system, see:
* https://github.com/bytedeco/javacpp
* https://github.com/opendiff/java-c-plus-plus
* https://github.com/bytedeco/gradle-javacpp
* https://github.com/illumon-public/JavaCppGradleTemplate/tree/master

This example is based off of the manual build at https://github.com/opendiff/java-c-plus-plus.

New versions of [./javacpp.jar](./javacpp.jar) can be downloaded from [https://github.com/bytedeco/javacpp](https://github.com/bytedeco/javacpp).  The file is contained in the zip file of a release.

## Building

To build this example, you will need to have the following installed:
* Java
* A C++ compiler

To build this example, run the following command:
```bash
./build.sh
```

## Running

The build will create a jar file and shared libraries in the `build` directory.  These plus [./javacpp.jar](./javacpp.jar) are needed to run.
You will need to specify the location of the shared libraries and add the jars to the class path when running the jar file.

Here is an example from the end of [./build.sh](./build.sh)
```bash
java -Djava.library.path=${OUTPUT_DIR} -cp ${OUTPUT_DIR}/blackscholes.jar:javacpp.jar org.example.Main
```

