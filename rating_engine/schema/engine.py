from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


class AuthorizationRequest(BaseModel):
    """Authorization request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    source: Optional[str] = None
    source_ip: Optional[str] = None
    destination: Optional[str] = None
    carrier_ip: Optional[str] = None
    tags: List[str] = []
    timestamp_auth: Optional[datetime] = None


class AuthorizationResponse(BaseModel):
    """Authorization response"""

    authorized: bool = False
    authorized_destination: bool = False
    unauthorized_account_tag: Optional[str] = None
    unauthorized_reason: Optional[str] = None
    balance: int = 0
    carriers: List[str] = []
    max_available_units: int = 0


class AuthorizationTransactionRequest(BaseModel):
    """Authorization transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    account_tags: List[str] = []
    destination_account_tag: Optional[str] = None
    destination_account_tags: List[str] = []
    source: Optional[str] = None
    source_ip: Optional[str] = None
    destination: Optional[str] = None
    carrier_ip: Optional[str] = None
    tags: List[str] = []
    timestamp_auth: datetime
    authorized: bool = False
    authorized_destination: bool = False
    unauthorized_account_tag: Optional[str] = None
    unauthorized_reason: Optional[str] = None
    balance: int = 0
    carriers: List[str] = []
    max_available_units: int = 0


class AuthorizationTransactionResponse(BaseModel):
    """Authorization transaction response"""

    ok: bool = False


class BeginTransactionRequest(BaseModel):
    """Begin transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    agent_tag: Optional[str] = None
    source: Optional[str] = None
    source_ip: Optional[str] = None
    destination: Optional[str] = None
    carrier_ip: Optional[str] = None
    tags: List[str] = []
    timestamp_begin: Optional[datetime] = None


class BeginTransactionResponse(BaseModel):
    """Begin transaction response"""

    ok: bool = False
    failed_account_tag: Optional[str] = None
    failed_reason: Optional[str] = None


class EndTransactionRequest(BaseModel):
    """End transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    timestamp_end: Optional[datetime] = None


class EndTransactionResponse(BaseModel):
    """Begin transaction response"""

    ok: bool = False
    failed_account_tag: Optional[str] = None
    failed_reason: Optional[str] = None


class RollbackTransactionRequest(BaseModel):
    """Rollback transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None


class RollbackTransactionResponse(BaseModel):
    """Begin transaction response"""

    ok: bool = False


class RecordTransactionRequest(BaseModel):
    """Record transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    source: Optional[str] = None
    source_ip: Optional[str] = None
    destination: Optional[str] = None
    carrier_ip: Optional[str] = None
    tags: List[str] = []
    authorized: bool = False
    unauthorized_reason: Optional[str] = None
    timestamp_auth: Optional[datetime] = None
    timestamp_begin: Optional[datetime] = None
    timestamp_end: Optional[datetime] = None


class RecordTransactionResponse(BaseModel):
    """Record transaction response"""

    ok: bool = False
