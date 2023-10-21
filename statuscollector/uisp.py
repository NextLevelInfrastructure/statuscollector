#!/usr/bin/env python3
"""
uisp.py

The UISP API as documented in https://unmscrm.docs.apiary.io/#introduction
and https://help.ui.com/hc/en-us/articles/115003906007-UISP-CRM-API-Usage
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


class NoServicePlan:
    def __init__(self, spid):
        self.owner, self.spid, self.values = None, spid, {}
        self.active_services = 0
        self.target_actives = 0
        self.total_price = 0

    def total_capitated_to_nli(self):
        return self.total_price

    def total_capitated_connectivity(self):
        return 0

    def remainder_after_nli_capitation(self):
        return 0


class ServicePlan:
    DEFAULT_BILLING_RATE = 0.03

    def __init__(self, owner, spid, values):
        self.owner, self.spid, self.values = owner, spid, values
        self.active_services = 0
        self.total_price = 0
        bi = values['billing_instructions'][spid]
        self.target_actives = bi['subscriber_target']
        self.management_per_service = bi['nli_management']
        self.isp_per_service = bi['nli_isp']
        self.connectivity_per_service = bi['nli_capitated_connectivity']
        self.billing_fee = bi.get('nli_billing_fee')

    def total_capitated_to_nli(self):
        per_service = self.management_per_service + self.isp_per_service
        billing_fee = (self.DEFAULT_BILLING_RATE * self.total_price) if self.billing_fee is None else (self.billing_fee * self.active_services)
        return per_service * self.active_services + billing_fee

    def total_capitated_connectivity(self):
        return self.connectivity_per_service * self.active_services

    def remainder_after_nli_capitation(self):
        return self.total_price - self.total_capitated_to_nli()


class Organizations:
    def __init__(self, config):
        self.config = config.get('organizations', {})
        self.spid2owner = {}
        for owner, d in self.config.items():
            for spid in (d['billing_instructions'] or {}).keys():
                self.spid2owner[spid] = ServicePlan(owner, spid, d)

    def get_owner(self, spid):
        return self.spid2owner.get(spid)

    def register_service(self, service):
        if service['status'] != ServiceStatus.ACTIVE.value:
            return
        spid = service['servicePlanId']
        owner = self.spid2owner.get(spid)
        if not owner:
            owner = NoServicePlan(spid)
            self.spid2owner[spid] = owner
        owner.active_services += 1
        owner.total_price += service['price']

def currency_str(v):
    vv = round(v, 2)
    return '${:.2f}'.format(vv)

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
        return self.bearer_json_request(requests.get, f'/clients?organizationId={organization["id"]}')

    def get_services_of(self, organization):
        return self.bearer_json_request(requests.get, f'/clients/services?organizationId={organization["id"]}')

    def name_of(self, client):
        return f'{client["firstName"]} {client["lastName"]}' if client['firstName'] else f'COMPANY:{client["companyName"]}, {client["companyContactFirstName"]} {client["companyContactLastName"]}' if client['companyContactFirstName'] else f'COMPANY:{client["companyName"]}' if client['companyName'] else str(client)

    def printable_client(self, client):
        b = client['accountBalance']
        name = self.name_of(client)
        balance = f' owes {currency_str(-b)}' if b < 0 else f' credit {currency_str(b)}' if b > 0 else ''
        active = '' if client['isActive'] else ' INACTIVE'
        autopay = '' if client['hasAutopayCreditCard'] else ' NO-AUTOPAY'
        overdue = ' OVERDUE' if client['hasOverdueInvoice'] else ''
        suspended = ' SUSPENDED' if client['hasSuspendedService'] else ''
        lead = ' LEAD' if client['isLead'] else ''
        invite = '' if client['invitationEmailSentDate'] else ' no-invite'
        return f'{client["username"]} {name}{balance}{autopay}{overdue}{active}{lead}{suspended}{invite}'


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
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if client['accountBalance'] > 0:
            print(uisp.printable_client(client), newservice)
    for client in clients:
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if client['accountBalance'] < 0:
            print(uisp.printable_client(client), newservice)
    for client in active_clients:
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if not client['accountBalance']:
            print(uisp.printable_client(client), newservice)
    for client in inactive_clients:
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if not client['accountBalance']:
            print(uisp.printable_client(client), newservice)


if __name__ == '__main__':
    import sys, yaml
    assert len(sys.argv) == 2, sys.argv
    config = yaml.safe_load(open(sys.argv[1]))
    organizations = Organizations(config)
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
        credit = f', {len(payables)} clients have total credit {currency_str(sum(payables))}' if payables else ''
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

        # for each service plan with at least one active service:
        #   * show the clients who are active on that service plan,
        #   * warn if the count of actives is below target,

        print(f'\n{org["name"]}: {len(active_clients)} active of {len(clients)} clients{archived}')
        # \n{len(receivables)} clients owe total {currency_str(-sum(receivables))}{credit}:\n')
        cids_with_service = set()
        nli_capitated_nonconnectivity = 0
        nli_capitated_connectivity = 0
        revenue_after_nli_capitated = 0
        values = {}
        for (spid, services) in this_month.idmap.items():
            for s in services:
                organizations.register_service(s)
            owner = organizations.get_owner(spid)
            values = values or owner.values
            nli_capitated_nonconnectivity += owner.total_capitated_to_nli()
            nli_capitated_connectivity += owner.total_capitated_connectivity()
            revenue_after_nli_capitated += owner.remainder_after_nli_capitation()
            warning = f' (WARNING less than target {owner.target_actives})' if owner.target_actives > owner.active_services else f' (target {owner.target_actives})'
            nli100 = '' if owner.values else f' 100% NLI' 
            dls = services[0]['downloadSpeed']
            speed = f' {int(dls)} Mbps' if dls else ''
            print(f'\n=== {services[0]["name"]}({spid}){speed} has {owner.active_services} actives{warning}{nli100}')
            cids = { s['clientId'] for s in services  }
            cids_with_service |= cids
            cids_with_new_service = { s['clientId'] for s in services if s['activeFrom'] >= today.isoformat()[0:8] }

            # ordinarily we print only clients who have service. if you also
            # want to print clients without service, uncomment the next line.
            #print_all_clients(clients, uisp, cids_with_new_service)
            print_all_clients([clientmap.idmap[cid][0] for cid in cids], uisp, cids_with_new_service)

        # A client was dropped from previous month to current month if that
        # client is in previous month drops but not in current month actives.

        dropped_clients = { s['clientId'] for a in last_month.idmap.values() for s in a } - { s['clientId'] for a in this_month.idmap.values() for s in a }
        for c in clients:
            if c['id'] in dropped_clients:
                print(f'**** {uisp.name_of(c)} no longer has service')
            if c['id'] in cids_with_service and not c['username']:
                LOGGER.warning(f'**** WARNING: client has no username: {uisp.name_of(c)}')

        fmp = values.get('fixed_monthly_payouts', [])
        fmpstr = (', ' + ', '.join(f'{p[0]} {currency_str(p[1])}' for p in fmp)) if fmp else ''
        nli_capped_connectivity = min(max(nli_capitated_connectivity, values.get('capitated_connectivity_min', 0)), values.get('capitated_connectivity_max', 100000000))
        monthly = values.get('nli_monthly_connectivity', 0)
        net = revenue_after_nli_capitated - monthly - nli_capped_connectivity - sum([p[1] for p in fmp])
        print(f'\n === NLI capitated nonconnectivity {currency_str(nli_capitated_nonconnectivity)}, NLI connectivity {currency_str(monthly + nli_capped_connectivity)}{fmpstr}, net to NLI {currency_str(nli_capitated_nonconnectivity + monthly + nli_capped_connectivity)}, net to customer {currency_str(net)}')

    print(f'\ngrand total receivable: {currency_str(-overall_receivable)}, grand total credit: {currency_str(overall_credit)}, net: {currency_str(-overall_receivable-overall_credit)}')
