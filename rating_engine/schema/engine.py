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
    destination: Optional[str] = None
    timestamp_auth: Optional[datetime] = None
    tags: List[str] = []


class AuthorizationResponse(BaseModel):
    """Authorization response"""

    authorized: bool = False
    authorized_destination: bool = False
    unauthorized_account_tag: Optional[str] = None
    unauthorized_account_reason: Optional[str] = None
    unauthorized_destination_reason: Optional[str] = None
    max_available_units: int = 0
    balance: int = 0
    carriers: List[str] = []


class AuthorizationTransactionRequest(BaseModel):
    """Authorization transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    source: Optional[str] = None
    destination: Optional[str] = None
    timestamp_auth: datetime
    tags: List[str] = []
    inbound: bool = False
    primary: bool = False
    authorized: bool = False
    authorized_destination: bool = False
    unauthorized_account_tag: Optional[str] = None
    unauthorized_account_reason: Optional[str] = None
    unauthorized_destination_reason: Optional[str] = None
    max_available_units: int = 0
    balance: int = 0
    carriers: List[str] = []


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
    timestamp_begin: Optional[datetime] = None


class BeginTransactionResponse(BaseModel):
    """Begin transaction response"""

    ok: bool = False
    failed_account_tag: Optional[str] = None
    failed_account_reason: Optional[str] = None


class EndTransactionRequest(BaseModel):
    """End transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    timestamp_end: Optional[datetime] = None
    source: Optional[str] = None
    source_ip: Optional[str] = None
    destination: Optional[str] = None
    carrier_ip: Optional[str] = None


class EndTransactionResponse(BaseModel):
    """Begin transaction response"""

    ok: bool = False
    failed_account_tag: Optional[str] = None
    failed_account_reason: Optional[str] = None


class RollbackTransactionRequest(BaseModel):
    """Rollback transaction request"""

    tenant: str = 'default'
    transaction_tag: str
    account_tag: Optional[str] = None
    destination_account_tag: Optional[str] = None
    source: Optional[str] = None
    source_ip: Optional[str] = None
    destination: Optional[str] = None
    carrier_ip: Optional[str] = None


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
    destination: Optional[str] = None
    timestamp_auth: Optional[datetime] = None
    timestamp_begin: Optional[datetime] = None
    timestamp_end: Optional[datetime] = None
    failed: bool = False
    failed_reason: Optional[str] = None


class RecordTransactionResponse(BaseModel):
    """Record transaction response"""

    ok: bool = False
