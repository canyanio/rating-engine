from enum import Enum


class MethodName(Enum):
    AUTHORIZATION = "authorization"
    AUTHORIZATION_TRANSACTION = "authorization_transaction"
    BEGIN_TRANSACTION = "begin_transaction"
    END_TRANSACTION = "end_transaction"
    ROLLBACK_TRANSACTION = "rollback_transaction"
    RECORD_TRANSACTION = "record_transaction"


class RPCCallPriority(Enum):
    LOW = 10
    MEDIUM = 20
    HIGH = 30
