#include "kv_store.grpc.pb.h"

typedef enum {
    INV,
    GET,
    PUT
} request_t;

class Request {
public:
    Request(std::string _addr, request_t _type, std::string _key, std::string _value):
        addr(_addr),
        type(_type),
        key(_key),
        value(_value)
    {}
    
    Request(std::string _addr, request_t _type, std::string _key):
        addr(_addr),
        type(_type),
        key(_key),
        value("")
    {}
    
    Request():
        addr(""),
        type(request_t::INV),
        key(""),
        value("")
    {}

    Request(getReq _req): Request(_req.meta().addr(), request_t::GET, _req.key()) {}
    Request(fwdGetReq _req): Request(_req.req().meta().addr(), request_t::GET, _req.req().key()) {}
    
    Request(putReq _req): Request(_req.meta().addr(), request_t::PUT, _req.key(), _req.value()) {}
    Request(fwdPutReq _req): Request(_req.req().meta().addr(), request_t::PUT, _req.req().key(), _req.req().value()) {}

    std::string addr;
    request_t type;
    std::string key;
    std::string value;
};
    
