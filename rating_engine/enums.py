from enum import Enum


class MethodName(Enum):
    AUTHORIZATION = "authorization"
    BEGIN_TRANSACTION = "begin_transaction"
    END_TRANSACTION = "end_transaction"
    ROLLBACK_TRANSACTION = "rollback_transaction"
    RECORD_TRANSACTION = "record_transaction"
