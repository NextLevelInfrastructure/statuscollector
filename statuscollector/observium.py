#!/usr/bin/env python3
"""
observium.py

The Observium API as documented in https://docs.observium.org/api/
"""

import datetime, json, logging, prometheus_client, requests, time

REQUEST_TIME = prometheus_client.Summary('observium_processing_seconds',
                                         'time of Observium API requests')
LOGGER = logging.getLogger('statuscollector.observium')


class ObserviumClientError(Exception):
    pass


class ObserviumClient:
    def __init__(self, config):
        self.config = config.get('observium', {})
        self.urlprefix = self.config.get('urlprefix')
        if not self.urlprefix:
            raise ObserviumClientError('no urlprefix in observium config')
        username = self.config.get('username')
        if not username:
            raise ObserviumClientError('no username in observium config')
        password = self.config.get('password')
        if not password:
            raise ObserviumClientError('no password in observium config')
        self.basicauth = requests.auth.HTTPBasicAuth(username, password)
        self.devices_querystring = self.config.get('devices_querystring')
        if not self.devices_querystring:
            raise ObserviumClientError('no devices_querystring in observium config')
        self.timeout = self.config.get('timeout', 10)

    @REQUEST_TIME.time()
    def bearer_json_request(self, command, path, data=None, json=None):
        endpoint = '%s%s' % (self.urlprefix, path)
        headers = { }
        if data: # depending on command, data may not be allowed as an argument
            resp = command(endpoint, headers=headers, timeout=self.timeout, data=data, auth=self.basicauth)
        elif json:
            resp = command(endpoint, headers=headers, timeout=self.timeout, json=json, auth=self.basicauth)
        else:
            resp = command(endpoint, headers=headers, timeout=self.timeout, auth=self.basicauth)
        resp.raise_for_status()
        if resp.status_code == 204:
            return None
        return resp.json()

    def get_devices(self):
        return self.bearer_json_request(requests.get, f'/devices/?{self.devices_querystring}')

    def get_ports(self, devicenum):
        return self.bearer_json_request(requests.get, f'/ports/?device_id={devicenum}&fields=ifAlias,ifSpeed,ifAdminStatus')
