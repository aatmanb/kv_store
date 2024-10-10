#include <pybind11/pybind11.h>
#include "739kv.h"
namespace py = pybind11;

PYBIND11_MODULE(libkv739_py, m) {
    m.doc() = "Python extension module for key-value store";

    m.def("init", &kv739_init, py::arg("server_name"), py::call_guard<py::gil_scoped_release>());

    m.def("shutdown", &kv739_shutdown, py::call_guard<py::gil_scoped_release>());

    m.def("get", &kv739_get, py::arg("key"), py::arg("value"), py::call_guard<py::gil_scoped_release>());

    m.def("put", &kv739_put, py::arg("key"), py::arg("value"), py::arg("old_value"), py::call_guard<py::gil_scoped_release>());
}