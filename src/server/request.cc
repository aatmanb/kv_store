#include "request.h"

std::string req_type_name(request_t req) {
    switch (req) {
        case request_t::INV: return "INV";
        case request_t::GET: return "GET";
        case request_t::PUT: return "PUT";
        default:           return "Unknown";
    }
}

Request::Request(std::string _addr, request_t _type, std::string _key, std::string _value, bool _retry):
    addr(_addr),
    type(_type),
    key(_key),
    value(_value),
    retry(_retry)
{}

Request::Request(std::string _addr, request_t _type, std::string _key, bool _retry):
    addr(_addr),
    type(_type),
    key(_key),
    value(""),
    retry(_retry)
{}

Request::Request():
    addr(""),
    type(request_t::INV),
    key(""),
    value(""),
    retry(false)
{}

Request::Request(std::string _addr, request_t _type, std::string _key, std::string _value):
    addr(_addr),
    type(_type),
    key(_key),
    value(_value)
{}

Request::Request(std::string _addr, request_t _type, std::string _key):
    addr(_addr),
    type(_type),
    key(_key),
    value("")
{}

Request::Request(getReq _req): Request(_req.meta().addr(), request_t::GET, _req.key(), _req.retry()) {}
Request::Request(fwdGetReq _req): Request(_req.req()) {}

Request::Request(putReq _req): Request(_req.meta().addr(), request_t::PUT, _req.key(), _req.value(), _req.retry()) {}
Request::Request(fwdPutReq _req): Request(_req.req()) {}
Request::Request(putAck _req): Request(_req.meta().addr(), request_t::PUT, _req.key(), _req.value()) {}

getReq 
Request::rpc_getReq() {
    assert(type == request_t::GET);
    getReq req;
    req.set_key(key);
    auto *meta = req.mutable_meta();
    meta->set_addr(addr);
    return req;
}

fwdGetReq
Request::rpc_fwdGetReq() {
    assert(type == request_t::GET);
    fwdGetReq req;
    auto *original_req = req.mutable_req();
    auto *meta = original_req->mutable_meta();
    original_req->set_key(key);
    meta->set_addr(addr);
    return req;
}

putReq
Request::rpc_putReq() {
    assert(type == request_t::PUT);
    putReq req;
    req.set_key(key);
    req.set_value(value);
    req.set_retry(retry);
    auto *meta = req.mutable_meta();
    meta->set_addr(addr);
    return req;
}

fwdPutReq
Request::rpc_fwdPutReq() {
    assert(type == request_t::PUT);
    fwdPutReq req;
    auto *original_req = req.mutable_req();
    auto *meta = original_req->mutable_meta();
    original_req->set_key(key);
    original_req->set_value(value);
    original_req->set_retry(retry);
    meta->set_addr(addr);
    return req;
}

putAck
Request::rpc_putAck() {
    assert(type == request_t::PUT);
    putAck req;
    req.set_key(key);
    req.set_value(value);
    auto *meta = req.mutable_meta();
    meta->set_addr(addr);
    return req;
}

bool
Request::identicalRequests(Request r2) {
    return (type == r2.type) && (addr == r2.addr) && 
           (key == r2.key) && (value == r2.value);
}

std::string
Request::dumpRequestInfo() {
    std::stringstream ss;
    ss << "Type: " << req_type_name(type) << ", Client Addr: " << addr << ", key: " << key << ", value: " << value; 
    return ss.str();
}