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
    Request(fwdGetReq _req): Request(_req.req()) {}
    
    Request(putReq _req): Request(_req.meta().addr(), request_t::PUT, _req.key(), _req.value()) {}
    Request(fwdPutReq _req): Request(_req.req()) {}
    Request(putAck _req): Request(_req.meta().addr(), request_t::PUT, _req.key(), _req.value()) {}

    getReq rpc_getReq() {
        assert(type == request_t::GET);
        getReq req;
        req.set_key(key);
        auto *meta = req.mutable_meta();
        meta->set_addr(addr);
        return req;
    }

    fwdGetReq rpc_fwdGetReq() {
        assert(type == request_t::GET);
        fwdGetReq req;
        auto *original_req = req.mutable_req();
        auto *meta = original_req->mutable_meta();
        original_req->set_key(key);
        meta->set_addr(addr);
        return req;
    }

    putReq rpc_putReq() {
        assert(type == request_t::PUT);
        putReq req;
        req.set_key(key);
        req.set_value(value);
        auto *meta = req.mutable_meta();
        meta->set_addr(addr);
        return req;
    }

    fwdPutReq rpc_fwdPutReq() {
        assert(type == request_t::PUT);
        fwdPutReq req;
        auto *original_req = req.mutable_req();
        auto *meta = original_req->mutable_meta();
        original_req->set_key(key);
        original_req->set_value(value);
        meta->set_addr(addr);
        return req;
    }

    putAck rpc_putAck() {
        assert(type == request_t::PUT);
        putAck req;
        req.set_key(key);
        req.set_value(value);
        auto *meta = req.mutable_meta();
        meta->set_addr(addr);
        return req;
    }

    bool identicalRequests(Request r2) {
        return (type == r2.type) && (addr == r2.addr) && 
               (key == r2.key) && (value == r2.value);
    }

    void dumpRequestInfo() {
        std::cout << "Type: " << type << ", Client Addr: " << addr << ", key: " << key << ", value: " << value << std::endl; 
    }

    std::string addr;
    request_t type;
    std::string key;
    std::string value;
};
