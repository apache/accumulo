namespace java org.apache.accumulo.core.security.thrift

enum SecurityErrorCode {
    DEFAULT_SECURITY_ERROR = 0,
    BAD_CREDENTIALS = 1,
    PERMISSION_DENIED = 2,
    USER_DOESNT_EXIST = 3,
    CONNECTION_ERROR = 4,
    USER_EXISTS = 5,
    GRANT_INVALID = 6,
    BAD_AUTHORIZATIONS = 7,
    INVALID_INSTANCEID = 8
}

struct AuthInfo {
    1:string user,
    2:binary password,
    3:string instanceId
}

exception ThriftSecurityException {
    1:string user,
    2:SecurityErrorCode code
}
