#include <pybind11/pybind11.h>
#include <exception>
#include <tuple>
#include "739kv.h"
namespace py = pybind11;

std::tuple<int, std::string> get(std::string &key) {
    std::string value;
    int status = kv739_get(key, value);
    return std::make_tuple(status, value);
}

std::tuple<int, std::string> put(std::string &key, std::string &value) {
    std::string old_value;
    int status = kv739_put(key, value, old_value);
    return std::make_tuple(status, value);
}

PYBIND11_MODULE(libkv739_py, m) {
    m.doc() = "Python extension module for key-value store";

    m.def("init", &kv739_init, py::arg("server_name"), py::call_guard<py::gil_scoped_release>());

    m.def("shutdown", &kv739_shutdown, py::call_guard<py::gil_scoped_release>());

    m.def("get", &get, py::arg("key"), py::call_guard<py::gil_scoped_release>());

    m.def("put", &put, py::arg("key"), py::arg("value"), py::call_guard<py::gil_scoped_release>());
}
