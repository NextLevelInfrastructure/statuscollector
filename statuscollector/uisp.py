#!/usr/bin/env python3
"""
uisp.py

The UISP API as documented in https://unmscrm.docs.apiary.io/#introduction
and https://help.ui.com/hc/en-us/articles/115003906007-UISP-CRM-API-Usage
"""

import json, logging, prometheus_client, requests, time, threading
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
        self.owners = { k for k in self.config.keys() }
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
            # Then this spid was not present in the config file, so
            # all funds are allocaated to NLI.
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
        active = '' ### if client['isActive'] else ' INACTIVE'
        autopay = '' if client['hasAutopayCreditCard'] else ' NO-AUTOPAY'
        pastdue = ' PAST-DUE' if client['hasOverdueInvoice'] else ''
        suspended = ' SUSPENDED' if client['hasSuspendedService'] else ''
        lead = ' LEAD' if client['isLead'] else ''
        invite = '' ### if client['invitationEmailSentDate'] else ' no-invite'
        return f'{client["username"]} {name}{balance}{autopay}{pastdue}{active}{lead}{suspended}{invite}'


def print_clients(clients, uisp, cids_with_new_service=set(), only=[]):
    ## active_clients = [c for c in clients if c['isActive'] and not c['isArchived'] and not c['isLead']]
    def matching(client):
        matches = 0
        if 'PAST-DUE' in only and client['hasOverdueInvoice']:
            matches += 1
        if 'NO-AUTOPAY' in only and not client['hasAutopayCreditCard']:
            matches += 1
        if 'INACTIVE' in only and not client['isActive']:
            matches += 1
        return matches == len(only)
    matching_clients = [c for c in clients if matching(c) and not c['isArchived'] and not c['isLead']]
    for client in matching_clients:
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if client['accountBalance'] > 0:
            print(uisp.printable_client(client), newservice)
    for client in matching_clients:
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if client['accountBalance'] < 0:
            print(uisp.printable_client(client), newservice)
    for client in matching_clients:
        newservice = 'NEWSERVICE' if client['id'] in cids_with_new_service else ''
        if not client['accountBalance']:
            print(uisp.printable_client(client), newservice)
