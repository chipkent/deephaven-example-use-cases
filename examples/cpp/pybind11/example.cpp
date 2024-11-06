#include <pybind11/pybind11.h>

double add(double a, double b) {
    return a + b;
}

PYBIND11_MODULE(example, m) {
    m.def("add", &add, "A function that adds two numbers");
}
