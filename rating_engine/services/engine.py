from ..schema import engine as schema
from ..enums import MethodName, RPCCallPriority
from . import api as api_service
from . import bus as bus_service
from . import rater as rater_service

from datetime import datetime
from pytz import timezone
from typing import Optional


UTC = timezone('UTC')


class EngineService(object):
    _api: api_service.APIService
    _bus: bus_service.BusService
    _rater: rater_service.RaterService

    def __init__(
        self, api: api_service.APIService, bus: bus_service.BusService, tz=None
    ):
        self._api = api
        self._bus = bus
        self._rater = rater_service.RaterService(tz=tz)

    def get_api(self) -> api_service.APIService:
        return self._api

    def set_api(self, api: api_service.APIService):
        self._api = api

    async def authorization(
        self, request: schema.AuthorizationRequest
    ) -> schema.AuthorizationResponse:
        # local timezone
        if request.timestamp_auth is None:
            request.timestamp_auth = UTC.localize(datetime.utcnow())
        # no account nor destination account specified
        if request.account_tag is None and request.destination_account_tag is None:
            return schema.AuthorizationResponse(authorized=False)
        # get the account and destination account
        (
            account,
            destination_account,
        ) = await self._api.get_account_and_destination_account_by_id(
            request.tenant,
            account_tag=request.account_tag,
            destination_account_tag=request.destination_account_tag,
            destination=request.destination,
        )
        # check the account
        if request.account_tag and account is None:
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.account_tag,
                unauthorized_reason='NOT_FOUND',
            )
        elif request.account_tag and account is not None and account['active'] is False:
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.account_tag,
                unauthorized_reason='NOT_ACTIVE',
            )
        elif (
            request.account_tag
            and account is not None
            and account['destination_rate'] is None
        ):
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.account_tag,
                unauthorized_reason='UNREACHEABLE_DESTINATION',
            )
        # check the destination account
        if request.destination_account_tag and destination_account is None:
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.destination_account_tag,
                unauthorized_reason='NOT_FOUND',
            )
        elif (
            request.destination_account_tag
            and destination_account is not None
            and destination_account['active'] is False
        ):
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.destination_account_tag,
                unauthorized_reason='NOT_ACTIVE',
            )
        # least cost routing
        carriers = (
            [
                "%(protocol)s:%(host)s:%(port)s" % carrier
                for carrier in account['least_cost_routing']
            ]
            if account is not None
            else []
        )
        # loop on account and its linked accounts
        balance = None
        authorization_response = None
        max_available_units = self._rater.MAX_UNITS_FOR_TRANSACTIONS
        for account_object, inbound in ((account, False), (destination_account, True)):
            if account_object is None:
                continue
            linked_accounts = account_object.pop('linked_accounts', [])
            for _, item in enumerate([account_object] + linked_accounts):
                # apply pending transactions to balance
                balance = item['balance']
                for pending_transaction in item['running_transactions']:
                    balance -= self._rater.get_transaction_fee(
                        transaction=pending_transaction
                    )
                # verify the max_concurrent_transactions attribute
                if item['max_concurrent_transactions'] is not None:
                    account_authorized = item['max_concurrent_transactions'] > len(
                        item['running_transactions']
                    )
                    if account_authorized is False:
                        authorization_response = schema.AuthorizationResponse(
                            unauthorized_account_tag=item['account_tag'],
                            unauthorized_reason='TOO_MANY_RUNNING_TRANSACTIONS',
                        )
                        break
                # calculate authorization and max_available_units
                if not inbound and item['type'] == 'PREPAID':
                    destination_rate = item['destination_rate']
                    res = self._rater.get_maximum_allowed_units_for_transaction(
                        balance, destination_rate
                    )
                    account_authorized, max_units = res
                    max_available_units = min(max_available_units, max_units)
                    if account_authorized is False:
                        authorization_response = schema.AuthorizationResponse(
                            unauthorized_account_tag=item['account_tag'],
                            unauthorized_reason='BALANCE_INSUFFICIENT',
                        )
                        break
            # authorization_response is populated if the auth failed, break
            if authorization_response is not None:
                break
        # authorization_response is none, auth succeeded
        if authorization_response is None:
            authorization_response = schema.AuthorizationResponse(
                authorized=bool(account),
                authorized_destination=bool(destination_account),
                balance=balance,
                carriers=carriers,
                max_available_units=max_available_units,
            )
        # record the authorization transaction (async)
        auth_tx_request = schema.AuthorizationTransactionRequest(
            tenant=request.tenant,
            transaction_tag=request.transaction_tag,
            account_tag=request.account_tag,
            destination_account_tag=request.destination_account_tag,
            source=request.source,
            source_ip=request.source_ip,
            destination=request.destination,
            carrier_ip=request.carrier_ip,
            timestamp_auth=request.timestamp_auth,
            tags=request.tags,
            authorized=authorization_response.authorized,
            authorized_destination=authorization_response.authorized_destination,
            balance=authorization_response.balance,
            carriers=authorization_response.carriers,
            max_available_units=authorization_response.max_available_units,
        )
        await self._bus.rpc_call(
            MethodName.AUTHORIZATION_TRANSACTION.value,
            dict(request=auth_tx_request.dict()),
            priority=RPCCallPriority.LOW,
        )
        # return the response
        return authorization_response

    async def authorization_transaction(
        self, request: schema.AuthorizationTransactionRequest
    ) -> schema.AuthorizationTransactionResponse:
        for (account_tag, authorized, inbound) in (
            (request.account_tag, request.authorized, False),
            (request.destination_account_tag, request.authorized_destination, True),
        ):
            if account_tag is None:
                continue
            response = await self._api.upsert_authorization_transaction(
                tenant=request.tenant,
                account_tag=account_tag,
                transaction=dict(
                    transaction_tag=request.transaction_tag,
                    source=request.source,
                    source_ip=request.source,
                    destination=request.destination,
                    carrier_ip=request.carrier_ip,
                    tags=request.tags,
                    timestamp_auth=request.timestamp_auth,
                    authorized=authorized,
                    unauthorized_reason=request.unauthorized_reason if request.unauthorized_account_tag == account_tag else None,
                    inbound=inbound,
                    primary=True,
                ),
            )
            if response is None:
                return schema.AuthorizationTransactionResponse(
                    failed_account_tag=account_tag,
                    failed_reason='INTERNAL_ERROR',
                )
        return schema.AuthorizationTransactionResponse(ok=True)

    async def _restore_transaction_state_from_auth_request(
        self, tenant: str, transaction_tag: str
    ) -> Optional[dict]:
        state: dict = {}
        txs = await self._api.get_primary_transactions_by_tenant_and_tag(
            tenant, transaction_tag
        )
        for tx in txs:
            state['account_tag'] = None
            state['destination_account_tag'] = None
            if not tx['inbound']:
                state['account_tag'] = tx['account_tag']
            elif tx['inbound']:
                state['destination_account_tag'] = tx['account_tag']
            state.setdefault('source', tx['source'])
            state.setdefault('source_ip', tx['source_ip'])
            state.setdefault('destination', tx['destination'])
            state.setdefault('carrier_ip', tx['carrier_ip'])
        return state if state != {} else None

    async def begin_transaction(
        self, request: schema.BeginTransactionRequest
    ) -> schema.BeginTransactionResponse:
        if request.timestamp_begin is None:
            request.timestamp_begin = UTC.localize(datetime.utcnow())
        # no account nor destination account specified, api look-up
        if request.account_tag is None and request.destination_account_tag is None:
            state = await self._restore_transaction_state_from_auth_request(
                request.tenant, request.transaction_tag
            )
            if state is not None:
                request.account_tag = state['account_tag']
                request.destination_account_tag = state['destination_account_tag']
                request.source = state['source']
                request.source_ip = state['source_ip']
                request.destination = state['destination']
                request.carrier_ip = state['carrier_ip']
        # still no account nor destination account specified, give up
        if request.account_tag is None and request.destination_account_tag is None:
            return schema.BeginTransactionResponse(ok=False)
        # get the account and destination account
        (
            account,
            destination_account,
        ) = await self._api.get_account_and_destination_account_by_id(
            request.tenant,
            account_tag=request.account_tag,
            destination_account_tag=request.destination_account_tag,
            destination=request.destination,
        )
        # check the account
        if request.account_tag and account is None:
            return schema.BeginTransactionResponse(
                failed_account_tag=request.account_tag,
                failed_reason='NOT_FOUND',
            )
        elif request.account_tag and account is not None and account['active'] is False:
            return schema.BeginTransactionResponse(
                failed_account_tag=request.account_tag,
                failed_reason='NOT_ACTIVE',
            )
        # check the destination account
        if request.destination_account_tag and destination_account is None:
            return schema.BeginTransactionResponse(
                failed_account_tag=request.destination_account_tag,
                failed_reason='NOT_FOUND',
            )
        elif (
            request.destination_account_tag
            and destination_account is not None
            and destination_account['active'] is False
        ):
            return schema.BeginTransactionResponse(
                failed_account_tag=request.destination_account_tag,
                failed_reason='NOT_ACTIVE',
            )
        # write the db with the transaction
        for account, inbound in ((account, False), (destination_account, True)):
            if account is None:
                continue
            linked_accounts = account.pop('linked_accounts', [])
            for n, item in enumerate([account] + linked_accounts):
                response = await self._api.begin_account_transaction(
                    tenant=request.tenant,
                    account_tag=item['account_tag'],
                    destination_rate=item.get('destination_rate')
                    if not inbound
                    else None,
                    transaction_tag=request.transaction_tag,
                    source=request.source,
                    source_ip=request.source_ip,
                    destination=request.destination,
                    carrier_ip=request.carrier_ip,
                    timestamp_begin=request.timestamp_begin,
                    inbound=inbound,
                    primary=(n == 0),
                )
                if response is None:
                    return schema.BeginTransactionResponse(
                        failed_account_tag=item['account_tag'],
                        failed_reason='INTERNAL_ERROR',
                    )

        return schema.BeginTransactionResponse(ok=True)

    async def rollback_transaction(
        self, request: schema.RollbackTransactionRequest
    ) -> schema.RollbackTransactionResponse:
        # no account nor destination account specified, api look-up
        if request.account_tag is None and request.destination_account_tag is None:
            state = await self._restore_transaction_state_from_auth_request(
                request.tenant, request.transaction_tag
            )
            if state is not None:
                request.account_tag = state['account_tag']
                request.destination_account_tag = state['destination_account_tag']
        # still no account nor destination account specified, give up
        if request.account_tag is None and request.destination_account_tag is None:
            return schema.RollbackTransactionResponse(ok=False)
        # write the db with the rollback of transaction
        ok = True
        for account_tag in (request.account_tag, request.destination_account_tag):
            if account_tag is not None:
                response = await self._api.rollback_account_transaction(
                    tenant=request.tenant,
                    account_tag=account_tag,
                    transaction_tag=request.transaction_tag,
                )
                ok = ok and bool(response)
        # return ok
        return schema.RollbackTransactionResponse(ok=ok)

    async def end_transaction(
        self, request: schema.EndTransactionRequest
    ) -> schema.EndTransactionResponse:
        if request.timestamp_end is None:
            request.timestamp_end = UTC.localize(datetime.utcnow())
        # no account nor destination account specified, api look-up
        if request.account_tag is None and request.destination_account_tag is None:
            state = await self._restore_transaction_state_from_auth_request(
                request.tenant, request.transaction_tag
            )
            if state is not None:
                request.account_tag = state['account_tag']
                request.destination_account_tag = state['destination_account_tag']
        # still no account nor destination account specified, give up
        if request.account_tag is None and request.destination_account_tag is None:
            return schema.EndTransactionResponse(ok=False)
        # get the account and destination account
        (
            account,
            destination_account,
        ) = await self._api.get_account_and_destination_account_by_id(
            request.tenant,
            account_tag=request.account_tag,
            destination_account_tag=request.destination_account_tag,
        )
        # check the account
        if request.account_tag and account is None:
            return schema.EndTransactionResponse(
                failed_account_tag=request.account_tag,
                failed_reason='NOT_FOUND',
            )
        # check the destination account
        if request.destination_account_tag and destination_account is None:
            return schema.EndTransactionResponse(
                failed_account_tag=request.destination_account_tag,
                failed_reason='NOT_FOUND',
            )
        # write the db with the end of transaction
        for account, _ in ((account, False), (destination_account, True)):
            if account is None:
                continue
            linked_accounts = account.pop('linked_accounts', [])
            for item in linked_accounts + [account]:
                transaction = await self._api.end_account_transaction(
                    tenant=request.tenant,
                    account_tag=item['account_tag'],
                    transaction_tag=request.transaction_tag,
                    timestamp_end=request.timestamp_end,
                )
                if transaction is None:
                    return schema.EndTransactionResponse(
                        failed_account_tag=item['account_tag'],
                        failed_reason='INTERNAL_ERROR',
                    )
                transaction['timestamp_end'] = request.timestamp_end
                fee, duration = self._rater.get_transaction_fee_and_duration(
                    transaction=transaction
                )
                upsert_transaction = await self._api.upsert_transaction(
                    request.tenant, item['account_tag'], transaction, duration, fee
                )
                if upsert_transaction is None:
                    return schema.EndTransactionResponse(
                        failed_account_tag=item['account_tag'],
                        failed_reason='INTERNAL_ERROR',
                    )
                commit_account_transaction = await self._api.commit_account_transaction(
                    request.tenant, item['account_tag'], request.transaction_tag, fee
                )
                if commit_account_transaction is None:
                    return schema.EndTransactionResponse(
                        failed_account_tag=item['account_tag'],
                        failed_reason='INTERNAL_ERROR',
                    )
        # return ok
        return schema.EndTransactionResponse(ok=True)

    async def record_transaction(
        self, request: schema.RecordTransactionRequest
    ) -> schema.RecordTransactionResponse:
        if request.timestamp_begin is None:
            request.timestamp_begin = UTC.localize(datetime.utcnow())
        if request.timestamp_end is None:
            request.timestamp_end = UTC.localize(datetime.utcnow())
        return schema.RecordTransactionResponse()
