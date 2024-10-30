#include <string>

#include "kv_store.grpc.pb.h"
#include "spdlog/include/spdlog/spdlog.h"

typedef enum {
    INV,
    GET,
    PUT
} request_t;

std::string req_type_name(request_t req);

class Request {
public:
    Request(std::string _addr, request_t _type, std::string _key, std::string _value, bool _retry);
    Request(std::string _addr, request_t _type, std::string _key, bool _retry);
    Request();
    Request(std::string _addr, request_t _type, std::string _key, std::string _value);
    Request(std::string _addr, request_t _type, std::string _key);
    Request(getReq _req);
    Request(fwdGetReq _req);
    Request(putReq _req);
    Request(fwdPutReq _req);
    Request(putAck _req);

    getReq rpc_getReq();
    fwdGetReq rpc_fwdGetReq();
    putReq rpc_putReq();
    fwdPutReq rpc_fwdPutReq();
    putAck rpc_putAck();
    bool identicalRequests(Request r2);
    void dumpRequestInfo();

    std::string addr;
    request_t type;
    std::string key;
    std::string value;
    bool retry;
};
