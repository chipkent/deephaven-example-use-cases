// file: src/main/cpp/myhello.cpp
#include <iostream>
#include "myhello.h"

// g++ -dynamiclib -o libmyhello.dylib myhello.cpp

void printHelloWorld2() {
    std::cout << "Hello, World!" << std::endl;
}