#!/usr/bin/env python3
# exporter.py - read subscriber state from UISP

import logging, threading, time, yaml
from collections import namedtuple
from datetime import datetime
from enum import Enum

import botocore, boto3, prometheus_client, requests.exceptions

from uisp import UispClient, Organizations, ClientStatus, ServiceStatus


LOGGER = logging.getLogger('statuscollector.exporter')


class InvalidStatusError(Exception):
    pass

class ClientState(Enum):
    ACTIVE   = 0
    ARCHIVED = 1
    LEAD     = 2


REQUEST_TIME = prometheus_client.Summary('uisp_net_request_seconds',
                                         'Time spent waiting for request')
EMAIL_SUCCESS = prometheus_client.Gauge('uisp_email_success_count',
                                        'number of emails sent successfully', ['org'])
EMAIL_ERRORS = prometheus_client.Gauge('uisp_email_error_count',
                                       'number of email sending errors', ['org'])


class UispGauge:
    def __init__(self, name, helptext, labelmap, idlabel, model, selector):
        """
        Model is a function of zero arguments that returns a map from id
        to dictionary. Each key of labelmap is expected to be a key of
        each model dictionary. The value of the idlabel key is expected
        to be unique among all model dictionaries.

        Selector is a function from model dictionary to the (numeric) value
        of the gauge.
        """
        assert idlabel in labelmap, (idlabel, labelmap)
        self.labels = sorted(labelmap.keys())
        self.idlabel, self.model, self.selector = idlabel, model, selector
        self.id2labelvalues_map = {}
        self.gauge = prometheus_client.Gauge(name, helptext, [labelmap[j] for j in self.labels])
        self.update()

    def update(self):
        """Interrogate the model and update all labels to match."""
        for new_model_dict in self.model().values():
            self._update(new_model_dict)

    def _update(self, new_kv):
        new_labelvalues = [new_kv.get(s, '') for s in self.labels]
        idvalue = new_kv[self.idlabel]
        old_labelvalues = self.id2labelvalues_map.get(idvalue)
        if old_labelvalues != new_labelvalues:
            self.id2labelvalues_map[idvalue] = new_labelvalues
            def _select():
                assert idvalue in self.model(), (self.labels, idvalue, self.model())
                return self.selector(self.model()[idvalue])
            self.gauge.labels(*new_labelvalues).set_function(_select)
            #self.gauge.labels(*new_labelvalues).set_function(lambda idvalue=idvalue: self.selector(self.model()[idvalue]))
            if old_labelvalues:
                self.gauge.remove(*old_labelvalues)

class UispClientGauge:
    def __init__(self, metric, metric_desc, copylabels, model, value_selector):
        """
        Model is a function of zero arguments that returns a map from id
        to dictionary.
        """
        labelmap = { k: k for k in copylabels }
        labelmap['userIdent'] = 'nlid'  # 'userIdent' is renamed nlid
        def _selector(model_dict):
            v = value_selector(model_dict)
            if v is None or v is False:
                return 0
            if v is True:
                return 1
            if isinstance(v, str):
                # then it should be a date 2023-10-03T00:00:00-0700
                return datetime.fromisoformat(v).timestamp()
            return v
        self.gauge = UispGauge(metric, metric_desc, labelmap, 'id', model, _selector)


class PrometheusWrapper:
    """
    uisp_service_status{id="1", serviceId="10", status="ACTIVE", ...} = ServiceStatus
    uisp_service_price{id="1", serviceId="10", status="ACTIVE"} = price
    uisp_service_from_date{id="1", serviceId="10"} = activeFrom
    uisp_service_to_date{id="1", serviceId="10"} = activeTo
    uisp_service_contract_end_date{id="1", serviceId="10"} = contractEndDate
    uisp_service_last_invoiced_date{id="1", serviceId="10"} = lastInvoicedDate
            # if not client['invitationEmailSentDate'] no invite was sent

    """
    MIN_UPDATE_INTERVAL = 60 * 60  # seconds
                            
    def __init__(self, config, emailday, emailhour):
        self.config, self.emailday, self.emailhour = config, emailday, emailhour
        self.organizations = Organizations(self.config)
        self.uisp = UispClient(self.config)
        self.uisporgs = self.uisp.get_organizations()
        
        self.last_update, self.last_email = 0, 0
        self.errors = 0
        self.lock = threading.Lock()
        self.awsclient = boto3.client(
            'ses',
            region_name=self.config['ses']['region'],
            aws_access_key_id=self.config['ses']['access_key'],
            aws_secret_access_key=self.config['ses']['secret_key']
        )
        self.id2client_map = {}
        self.id2service_map = {}
        self._maybe_refresh()

        clientlabels = [
            'id', 'isLead', 'clientType', 'companyName', 'street1',
            'street2', 'city', 'countryId', 'stateId', 'zipCode',
            'organizationId', 'companyContactFirstName',
            'companyContactLastName', 'isActive', 'firstName', 'lastName',
            'username', 'isArchived', 
        ]
        def _clientmodel():
            self._maybe_refresh()
            return self.id2client_map
        def _stateselector(model_dict):
            return ClientStatus.from_client(model_dict).value
        self.clientstate_g = UispClientGauge('uisp_client_state', 'UISP client state', clientlabels, _clientmodel, _stateselector)
        contactlabels = ['id', 'clientId', 'email', 'phone', 'name', 'types']
        def _contactmodel():
            self._maybe_refresh()
            return { contact['id']: dict(contact, userIdent=client['userIdent'], types=','.join(sorted([t['name'] for t in contact['types']]))) for client in self.id2client_map.values() for contact in client['contacts'] }
        self.clientcontact_g = UispClientGauge('uisp_client_contact', 'UISP client contact info', contactlabels, _contactmodel, lambda model_dict: 1)
        self.clientbalance_g = UispClientGauge('uisp_client_balance', 'UISP client balance, negative means client owes us', ['id', 'currencyCode'], _clientmodel, lambda d: d['accountBalance'])
        self.clientpastdue_g = UispClientGauge('uisp_client_pastdue', 'UISP client pastdue balance', ['id'], _clientmodel, lambda d: d['hasOverdueInvoice'])
        self.clientautopay_g = UispClientGauge('uisp_client_autopay', 'UISP client has autopay enabled', ['id'], _clientmodel, lambda d: d['hasAutopayCreditCard'])
        self.clientinvited_g = UispClientGauge('uisp_client_invited_ts', 'UISP client invitation timestamp', ['id'], _clientmodel, lambda d: d['invitationEmailSentDate'])
        self.clientregistered_g = UispClientGauge('uisp_client_registered_ts', 'UISP client registration timestamp', ['id'], _clientmodel, lambda d: d['registrationDate'])

        servicelabels = [
            'id', 'clientId', 'prepaid', 'addressGpsLat', 'addressGpsLon',
            'servicePlanId', 'hasIndividualPrice',
            'downloadSpeed', 'uploadSpeed'
        ]
        def _servicemodel():
            self._maybe_refresh()
            return self.id2service_map
        self.servicestate_g = UispClientGauge('uisp_service_state', 'UISP service state', servicelabels, _servicemodel, lambda d: d['status'])
        self.serviceactivefrom_g = UispClientGauge('uisp_service_active_from_ts', 'UISP service start timestamp', ['id', 'clientId'], _servicemodel, lambda d: d['activeFrom'])
        self.serviceactiveto_g = UispClientGauge('uisp_service_active_to_ts', 'UISP service end timestamp, 0=ongoing', ['id', 'clientId'], _servicemodel, lambda d: d['activeTo'])
        self.servicecontractend_g = UispClientGauge('uisp_service_contract_end_ts', 'UISP service contract end timestamp, 0=no contract', ['id', 'clientId'], _servicemodel, lambda d: d['contractEndDate'])
        self.serviceinvoiced_g = UispClientGauge('uisp_service_last_invoiced_ts', 'UISP service contract last invoiced timestamp, 0=never invoiced', ['id', 'clientId'], _servicemodel, lambda d: d['lastInvoicedDate'])

        self.errors_g = prometheus_client.Gauge('uisp_errors', 'Number of errors')
        self.errors_g.set_function(lambda: self.errors)

    def _maybe_refresh(self):
        with self.lock:
            if time.time() - self.last_update > self.MIN_UPDATE_INTERVAL:
                try:
                    self._refresh()
                    self.last_update = time.time()
                except requests.exceptions.ReadTimeout:
                    LOGGER.exception()
                    self.errors += 1
        now = datetime.utcnow()
        if (self.emailday == now.weekday() and self.emailhour <= now.hour and
            time.time() - self.last_email > 3600*12):
            try:
                self._send_email()
                self.last_email = self.last_update
            except botocore.exceptions.ClientError:
                EMAIL_ERRORS.labels(organization='UNKNOWN').inc()
                LOGGER.exception()

    def _send_email(self):
        # see https://codelovingyogi.medium.com/sending-emails-using-aws-simple-email-service-ses-220de9db4fc8
        for (name, d) in self.config['organizations']:
            report_to = d.get('pastdue_report_to')
            if report_to:
                if isinstance(report_to, list):
                    dests = report_to
                elif isinstance(report_to, str):
                    dests = [report_to]
                else:
                    assert False, f'pastdue_report_to { report_to } for { name } has bad type'
                dests = ['dulitz@gmail.com'] ### FIXME
                body = f"""Hello { name } folks,

This is your periodic summary of FIXME!!!
"""
                response = self.awsclient.send_email(
                    Destination={ 'ToAddresses': dests },
                    Message={
                        'Body': {
                            'Text': {
                                'Charset': 'UTF-8',
                                'Data': body,
                            },
                        },
                        'Subject': {
                            'Charset': 'UTF-8',
                            'Data': f'Next Level Infrastructure subscriber summary for { name }',
                        },
                    },
                    Source='support@nextlevel.net'
                )
                if 'Error' in response:
                    EMAIL_ERRORS.labels(organization=name).inc()
                    LOGGER.error(f'failed to send email to { ", ".join(dests) }: { response["Error"] }')
                else:
                    EMAIL_SUCCESS.labels(organization=name).inc()
                    LOGGER.info(f'sent email to { ", ".join(dests) }')

    @REQUEST_TIME.time()
    def _refresh(self):
        self.serviceplans = { plan['id']: plan for plan in self.uisp.get_service_plans() }
        for org in self.uisporgs:
            LOGGER.info(f'refreshing UISP organization {org["name"]}')
            orgid = org['id']
            clients = self.uisp.get_clients_of(org)
            self.id2client_map = { c['id']: c for c in clients }
            services = self.uisp.get_services_of(org)
            for s in services:
                self.id2service_map[s['id']] = dict(s, userIdent=self.id2client_map.get(s['clientId'], { 'userIdent': -1 })['userIdent'], downloadSpeed=self.serviceplans[s['servicePlanId']].get('downloadSpeed', -1), uploadSpeed=self.serviceplans[s['servicePlanId']].get('uploadSpeed', -1))



def main(args):
    import argparse
    parser = argparse.ArgumentParser(
        prog=args[0],
        description='read UISP subscriber data',
        epilog='',
        allow_abbrev=False
    )
    parser.add_argument('config', type=str, help='yaml configuration file')
    parser.add_argument('--port', type=int, default=0, help='TCP port on which to serve prometheus /metrics')
    parser.add_argument('--emailday', type=int, default=-1, help='UTC day of week on which to send summary email (-1=none, 0=Sunday)')
    parser.add_argument('--emailhour', type=int, default=14, help='UTC hour in day to send summary email')
    vals = parser.parse_args(args=args[1:])

    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    LOGGER.info(f'main() {" ".join(args)}')

    config = yaml.safe_load(open(vals.config))

    if vals.port:
        wrapper = PrometheusWrapper(config, vals.emailday, vals.emailhour)
        LOGGER.info(f'serving metrics on port {vals.port}')
        prometheus_client.start_http_server(vals.port)
        while True:
            time.sleep(3600)
    else:
        LOGGER.warning('--port option not specified; exiting')
    return 1


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
