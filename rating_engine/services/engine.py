from ..schema import engine as schema
from . import api as api_service
from . import rater as rater_service

from datetime import datetime
from pytz import timezone


UTC = timezone('UTC')


class EngineService(object):
    _api: api_service.APIService
    _rater: rater_service.RaterService

    def __init__(self, api: api_service.APIService, tz=None):
        self._api = api
        self._rater = rater_service.RaterService(tz=tz)

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
                unauthorized_account_reason='NOT_FOUND',
            )
        elif request.account_tag and account is not None and account['active'] is False:
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.account_tag,
                unauthorized_account_reason='NOT_ACTIVE',
            )
        elif (
            request.account_tag
            and account is not None
            and account['destination_rate'] is None
        ):
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.account_tag,
                unauthorized_account_reason='UNREACHEABLE_DESTINATION',
            )
        # check the destination account
        if request.destination_account_tag and destination_account is None:
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.destination_account_tag,
                unauthorized_account_reason='NOT_FOUND',
            )
        elif (
            request.destination_account_tag
            and destination_account is not None
            and destination_account['active'] is False
        ):
            return schema.AuthorizationResponse(
                unauthorized_account_tag=request.destination_account_tag,
                unauthorized_account_reason='NOT_ACTIVE',
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
        max_available_units = self._rater.MAX_UNITS_FOR_TRANSACTIONS
        for account_objecft, inbound in ((account, False), (destination_account, True)):
            if account_objecft is None:
                continue
            linked_accounts = account_objecft.pop('linked_accounts', [])
            for item in linked_accounts + [account_objecft]:
                # apply pending transactions to balance
                balance = item['balance']
                for pending_transaction in item['running_transactions']:
                    print(pending_transaction)
                    balance -= self._rater.get_transaction_fee(
                        transaction=pending_transaction
                    )
                # verify the max_concurrent_transactions attribute
                if item['max_concurrent_transactions'] is not None:
                    account_authorized = item['max_concurrent_transactions'] > len(
                        item['running_transactions']
                    )
                    if account_authorized is False:
                        return schema.AuthorizationResponse(
                            unauthorized_account_tag=item['account_tag'],
                            unauthorized_account_reason='TOO_MANY_RUNNING_TRANSACTIONS',
                        )
                # calculate authorization and max_available_units
                if not inbound and item['type'] == 'PREPAID':
                    destination_rate = item['destination_rate']
                    res = self._rater.get_maximum_allowed_units_for_transaction(
                        balance, destination_rate
                    )
                    account_authorized, max_units = res
                    max_available_units = min(max_available_units, max_units)
                    if account_authorized is False:
                        return schema.AuthorizationResponse(
                            unauthorized_account_tag=item['account_tag'],
                            unauthorized_account_reason='BALANCE_INSUFFICIENT',
                        )
        # return the response
        return schema.AuthorizationResponse(
            authorized=bool(account),
            authorized_destination=bool(destination_account),
            balance=balance,
            max_available_units=max_available_units,
            carriers=carriers,
        )

    async def begin_transaction(
        self, request: schema.BeginTransactionRequest
    ) -> schema.BeginTransactionResponse:
        if request.timestamp_begin is None:
            request.timestamp_begin = UTC.localize(datetime.utcnow())
        # no account nor destination account specified
        if request.account_tag is None and request.destination_account_tag is None:
            return schema.BeginTransactionResponse(ok=False,)
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
                failed_account_reason='NOT_FOUND',
            )
        elif request.account_tag and account is not None and account['active'] is False:
            return schema.BeginTransactionResponse(
                failed_account_tag=request.account_tag,
                failed_account_reason='NOT_ACTIVE',
            )
        # check the destination account
        if request.destination_account_tag and destination_account is None:
            return schema.BeginTransactionResponse(
                failed_account_tag=request.destination_account_tag,
                failed_account_reason='NOT_FOUND',
            )
        elif (
            request.destination_account_tag
            and destination_account is not None
            and destination_account['active'] is False
        ):
            return schema.BeginTransactionResponse(
                failed_account_tag=request.destination_account_tag,
                failed_account_reason='NOT_ACTIVE',
            )
        # write the db with the transaction
        for account, inbound in ((account, False), (destination_account, True)):
            if account is None:
                continue
            linked_accounts = account.pop('linked_accounts', [])
            for item in linked_accounts + [account]:
                await self._api.begin_account_transaction(
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
                )
        return schema.BeginTransactionResponse(ok=True)

    async def rollback_transaction(
        self, request: schema.RollbackTransactionRequest
    ) -> schema.RollbackTransactionResponse:
        # write the db with the rollback of transaction
        response = await self._api.rollback_account_transaction(
            tenant=request.tenant,
            account_tag=request.account_tag,
            destination_account_tag=request.destination_account_tag,
            transaction_tag=request.transaction_tag,
        )
        # return ok
        return schema.RollbackTransactionResponse(ok=bool(response))

    async def end_transaction(
        self, request: schema.EndTransactionRequest
    ) -> schema.EndTransactionResponse:
        if request.timestamp_end is None:
            request.timestamp_end = UTC.localize(datetime.utcnow())
        # no account nor destination account specified
        if request.account_tag is None and request.destination_account_tag is None:
            return schema.EndTransactionResponse(ok=False,)
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
                failed_account_reason='NOT_FOUND',
            )
        # check the destination account
        if request.destination_account_tag and destination_account is None:
            return schema.EndTransactionResponse(
                failed_account_tag=request.destination_account_tag,
                failed_account_reason='NOT_FOUND',
            )
        # write the db with the end of transaction
        ok = True
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
                    ok = False
                    continue
                transaction['timestamp_end'] = request.timestamp_end
                fee, duration = self._rater.get_transaction_fee_and_duration(
                    transaction=transaction
                )
                await self._api.upsert_transaction(
                    request.tenant, item['account_tag'], transaction, duration, fee
                )
                await self._api.commit_account_transaction(
                    request.tenant, item['account_tag'], request.transaction_tag, fee
                )
        # return ok
        return schema.EndTransactionResponse(ok=ok)

    async def record_transaction(
        self, request: schema.RecordTransactionRequest
    ) -> schema.RecordTransactionResponse:
        if request.timestamp_begin is None:
            request.timestamp_begin = UTC.localize(datetime.utcnow())
        if request.timestamp_end is None:
            request.timestamp_end = UTC.localize(datetime.utcnow())
        return schema.RecordTransactionResponse()
