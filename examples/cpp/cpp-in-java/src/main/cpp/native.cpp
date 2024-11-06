#include <jni.h>

extern "C" JNIEXPORT jstring JNICALL Java_NativeLibrary_hello(JNIEnv* env, jobject obj) {
    return env->NewStringUTF("Hello from C++");
}
