#!/usr/bin/env python3
"""
uisp.py

The UISP API.
"""

import datetime, json, logging, prometheus_client, requests, time, threading
from enum import Enum

REQUEST_TIME = prometheus_client.Summary('uisp_processing_seconds',
                                         'time of UISP API requests')
LOGGER = logging.getLogger('statuscollector.uisp')

class UispClientError(Exception):
    pass

class ServiceStatus(Enum):
    PREPARED  = 0
    ACTIVE    = 1
    ENDED     = 2
    SUSPENDED = 3
    PREPARED_BLOCKED = 4
    OBSOLETE  = 5
    DEFERRED  = 6
    QUOTED    = 7
    INACTIVE  = 8


class UispClient:
    def __init__(self, config):
        self.config = config.get('uisp', {})
        self.urlprefix = self.config.get('urlprefix')
        if not self.urlprefix:
            raise UispClientError('no urlprefix in uisp config')
        self.apikey = self.config.get('apikey')
        if not self.apikey:
            raise UispClientError('no apikey in uisp config')
        self.timeout = self.config.get('timeout', 10)

    @REQUEST_TIME.time()
    def bearer_json_request(self, command, path, data=None, json=None):
        endpoint = '%s%s' % (self.urlprefix, path)
        headers = { 'X-Auth-App-Key': self.apikey }
        if data: # depending on command, data may not be allowed as an argument
            resp = command(endpoint, headers=headers, timeout=self.timeout, data=data)
        elif json:
            resp = command(endpoint, headers=headers, timeout=self.timeout, json=json)
        else:
            resp = command(endpoint, headers=headers, timeout=self.timeout)
        resp.raise_for_status()
        if resp.status_code == 204:
            return None
        return resp.json()

    def get_organizations(self):
        return self.bearer_json_request(requests.get, f'/organizations')

    def get_clients_of(self, organization):
        return uisp.bearer_json_request(requests.get, f'/clients?organizationId={organization["id"]}')

    def get_services_of(self, organization):
        return uisp.bearer_json_request(requests.get, f'/clients/services?organizationId={organization["id"]}')

    def name_of(self, client):
        return f'{client["firstName"]} {client["lastName"]}' if client['firstName'] else f'COMPANY:{client["companyName"]}, {client["companyContactFirstName"]} {client["companyContactLastName"]}' if client['companyContactFirstName'] else f'COMPANY:{client["companyName"]}' if client['companyName'] else str(client)

    def printable_client(self, client):
        b = client['accountBalance']
        name = self.name_of(client)
        balance = f' owes ${-b}' if b < 0 else f' credit ${b}' if b > 0 else ''
        active = '' if client['isActive'] else ' INACTIVE'
        overdue = ' OVERDUE' if client['hasOverdueInvoice'] else ''
        suspended = ' SUSPENDED' if client['hasSuspendedService'] else ''
        lead = ' LEAD' if client['isLead'] else ''
        invite = '' if client['invitationEmailSentDate'] else ' NOINVITE'
        return f'{client["username"]} {name}{balance}{overdue}{active}{lead}{suspended}{invite}'

class IdMapper:
    """Creates a map from an ID to a list of objects having that ID."""
    def __init__(self, services, idattribute):
        self.idmap = {}
        for s in services:
            spid = s[idattribute]
            sofar = self.idmap.get(spid, [])
            if not sofar:
                self.idmap[spid] = sofar
            sofar.append(s)


def print_all_clients(clients, uisp, cids_with_new_service=set()):
    active_clients = [c for c in clients if c['isActive'] and not c['isArchived'] and not c['isLead']]
    inactive_clients = [c for c in clients if not c['isActive'] and not c['isArchived'] and not c['isLead']]
    for client in clients:
        newservice = ' NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if client['accountBalance'] > 0:
            print(uisp.printable_client(client), newservice)
    for client in clients:
        newservice = ' NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if client['accountBalance'] < 0:
            print(uisp.printable_client(client), newservice)
    for client in active_clients:
        newservice = ' NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if not client['accountBalance']:
            print(uisp.printable_client(client), newservice)
    for client in inactive_clients:
        newservice = ' NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if not client['accountBalance']:
            print(uisp.printable_client(client), newservice)


if __name__ == '__main__':
    import sys, yaml
    assert len(sys.argv) == 2, sys.argv
    config = yaml.safe_load(open(sys.argv[1]))
    uisp = UispClient(config)

    overall_receivable, overall_credit = 0, 0
    orgs = uisp.get_organizations()
    for org in orgs:
        clients = uisp.get_clients_of(org)
        active_clients = [c for c in clients if c['isActive'] and not c['isArchived'] and not c['isLead']]
        archived_clients = [c for c in clients if c['isArchived']]
        archived = f' ({len(archived_clients)} archived)' if archived_clients else ''
        payables = [c['accountBalance'] for c in clients if c['accountBalance'] > 0]
        overall_credit += sum(payables)
        credit = f', {len(payables)} clients have total credit ${sum(payables)}' if payables else ''
        receivables = [c['accountBalance'] for c in clients if c['accountBalance'] < 0]
        overall_receivable += sum(receivables)
        services = uisp.get_services_of(org)
        active_services = [s for s in services if s['status'] == ServiceStatus.ACTIVE.value]
        today = datetime.date.today()
        lastmonth = (today - datetime.timedelta(days=14+today.day)).isoformat()
        ended_services = [s for s in services if s['status'] == ServiceStatus.ENDED.value and (s['activeTo'] or '').startswith(lastmonth[0:8])]

        clientmap = IdMapper(clients, 'id')
        this_month = IdMapper(active_services, 'servicePlanId')
        last_month = IdMapper(ended_services, 'servicePlanId')
        last_month_cids = IdMapper(ended_services, 'clientId')

        print(f'\n{org["name"]} {org["email"]} {len(active_clients)} active of {len(clients)} clients{archived}')
        # \n{len(receivables)} clients owe total ${-sum(receivables)}{credit}:\n')
        for (spid, services) in this_month.idmap.items():
            print(f'=== {services[0]["name"]}({spid}) has {len(services)} actives this month')
            cids = { s['clientId'] for s in services  }
            cids_with_new_service = { s['clientId'] for s in services if s['activeFrom'] >= today.isoformat()[0:8] }

            print_all_clients([clientmap.idmap[cid][0] for cid in cids], uisp, cids_with_new_service)

        dropped_clients = { s['clientId'] for a in last_month.idmap.values() for s in a } - { s['clientId'] for a in this_month.idmap.values() for s in a }
        for c in clients:
            if c['id'] in dropped_clients:
                print(f'**** {uisp.name_of(c)} no longer has service')
            if not c['username']:
                LOGGER.warning(f'**** client has no username: {uisp.name_of(c)}')

    print(f'\ngrand total receivable: ${-overall_receivable}, grand total credit: ${overall_credit}, net: ${-overall_receivable-overall_credit}')

    def foobar(cli):
        print(f'{uisp.name_of(cli)} active {service["servicePlanName"]} at {service["downloadSpeed"]}')
        for client in active_clients:
            if client['id'] not in clientids_with_active_service:
                found = False
                for ss in services:
                    if ss['clientId'] == client['id']:
                        found = True
                        sstat = f'{ss["name"]} ENDED {ss["activeTo"]}' if ss["status"] == 2 else str(ss)
                        LOGGER.warning(f'**** {uisp.name_of(client)}({client["id"]}) has no service, {sstat}')
                if not found:
                    LOGGER.warning(f'**** {uisp.name_of(client)}({client["id"]}) has no service')

# for each service plan with at least one active service:
#   * show the clients who are active on that service plan,
#   * warn if the count of actives is below target,
#   * show monthly allocation to NLI versus MFC
#   * show total receivables/payables as of current date

# a client was dropped between previous month and current month if
# that client is in previous month drops but not in current month actives.

# a client was added between previous month and current month if
# that client is not in previous month drops and is in current month
# actives and the service begins in the current month.

# servicePlanId, activeFrom, activeTo, status, clientId, name

# service:
# {'id': 324, 'prepaid': False, 'clientId': 164, 'status': 1, 'name': 'LAHCF Gigabit Internet - Residential', 'fullAddress': '25960 Quail Ln', 'street1': '25960 Quail Lane', 'street2': None, 'city': 'Los Altos Hills', 'countryId': 249, 'stateId': 5, 'zipCode': '94022', 'note': None, 'addressGpsLat': 37.376033333333, 'addressGpsLon': -122.12830544444, 'servicePlanId': 9, 'servicePlanPeriodId': 44, 'price': 155.0, 'hasIndividualPrice': False, 'totalPrice': 155.0, 'currencyCode': 'USD', 'invoiceLabel': None, 'contractId': None, 'contractLengthType': 1, 'minimumContractLengthMonths': None, 'activeFrom': '2023-10-02T00:00:00-0700', 'activeTo': None, 'contractEndDate': None, 'discountType': 0, 'discountValue': None, 'discountInvoiceLabel': 'Discount', 'discountFrom': None, 'discountTo': None, 'tax1Id': None, 'tax2Id': None, 'tax3Id': None, 'invoicingStart': '2023-10-01T00:00:00-0700', 'invoicingPeriodType': 2, 'invoicingPeriodStartDay': 1, 'nextInvoicingDayAdjustment': 10, 'invoicingProratedSeparately': True, 'invoicingSeparately': False, 'sendEmailsAutomatically': None, 'useCreditAutomatically': True, 'servicePlanName': 'LAHCF Gigabit Internet - Residential', 'servicePlanPrice': 155.0, 'servicePlanPeriod': 1, 'servicePlanType': 'Internet', 'downloadSpeed': 10000.0, 'uploadSpeed': 10000.0, 'hasOutage': False, 'unmsClientSiteStatus': None, 'fccBlockId': '060855117012002', 'lastInvoicedDate': '2023-10-31T00:00:00-0700', 'unmsClientSiteId': 'b07fb1f3-9db7-459b-bf2a-ff671c009ebb', 'attributes': [], 'addressData': None, 'suspensionReasonId': None, 'serviceChangeRequestId': None, 'setupFeePrice': None, 'earlyTerminationFeePrice': None, 'downloadSpeedOverride': None, 'uploadSpeedOverride': None, 'trafficShapingOverrideEnd': None, 'trafficShapingOverrideEnabled': False, 'servicePlanGroupId': None}
