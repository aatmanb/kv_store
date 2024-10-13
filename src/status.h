typedef enum {
    FAILURE = -1,
    GET_SUCCESS = 0,
    GET_FAILED  = 1,
    UPDATE_SUCCESS = 2,
    PUT_SUCCESS = 3,
    PUT_RECEIVED = 4,
    PUT_REDIRECT = 5,

    FAIL,
    REDIRECT,
    SUCCESS
} req_status_t
