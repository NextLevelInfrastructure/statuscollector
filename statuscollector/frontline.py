#!/usr/bin/python3
"""
frontline.py

Client for the Frontline API as documented in
    https://partnersupport.plume.com/s/article/Plume-Portal-M2M-API-Credential-Generation

"""

import json, logging, prometheus_client, requests, string, time, threading
import datetime, itertools, math, yaml

from uisp import UispClient, Organizations
from main import IdMapper


REQUEST_TIME = prometheus_client.Summary('frontline_processing_seconds',
                                         'time of Frontline API requests')
LOGGER = logging.getLogger('statuscollector.frontline')


def panic(message):
    import pdb, sys
    if sys.stdout.isatty():
        LOGGER.error(message)
        pdb.set_trace()
    else:
        LOGGER.fatal(message)
        sys.exit(1)


class FrontlineClientError(Exception):
    pass


class FrontlineClient:
    def __init__(self, config):
        self.config = config.get('frontline', {})
        self.urlprefix = self.config.get('urlprefix')
        if not self.urlprefix:
            raise FrontlineClientError('no urlprefix in frontline config')
        self.partnerid = self.config.get('partnerid')
        if not self.partnerid:
            raise FrontlineClientError('no partnerid in frontline config')
        self.authtoken = self.config.get('authtoken')
        if not self.authtoken:
            raise FrontlineClientError('no authtoken in frontline config')
        self.authurl = self.config.get('authurl')
        if not self.authurl:
            raise FrontlineClientError('no authurl in frontline config')
        self.authbody = self.config.get('authbody')
        if not self.authbody:
            raise FrontlineClientError('no authbody in frontline config')
        self.timeout = self.config.get('timeout', 10)
        self.jwt = self._bearer_jwt_request()

    @REQUEST_TIME.time()
    def _bearer_jwt_request(self):
        """POST to authurl with Authorization: header having value authtoken
        in the body use authbody. Returns JWT expiring in 720 min"""
        LOGGER.info(f'presenting authtoken to refresh JWT')
        self.jwt_request_time = time.time()
        headers = { 'Authorization': self.authtoken }
        resp = requests.post(self.authurl, headers=headers,
                             timeout=self.timeout, data=self.authbody)
        resp.raise_for_status()
        if resp.status_code == 204:
            return None
        return resp.json()

    @REQUEST_TIME.time()
    def bearer_json_request(self, command, path, data=None, json=None):
        if time.time() - self.jwt_request_time > self.jwt['expires_in'] / 2:
            self.jwt = self._bearer_jwt_request()
        endpoint = '%s%s' % (self.urlprefix, path)
        headers = { 'Authorization': f'Bearer {self.jwt["access_token"]}' }
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

    def get_customers_by_filter(self, jsonfilter=None):
        filt = f'?filter={jsonfilter}' if jsonfilter else ''
        return self.bearer_json_request(requests.get, '/Customers' + filt)

    def get_customers_by_partnerlabel(self, keyword):
        return self.bearer_json_request(requests.get, f'/partners/customers/search/{keyword}')
        
    def search_customers_by_name(self, name, exact=False, limit=30, skip=0):
        return self.bearer_json_request(requests.get, f'/partners/customers/search/{name}?field=name&exactMatch={"true" if exact else "false"}&limit={limit}&skip={skip}')

    def get_nodes_by_customerid(self, customerid, locationid):
        return self.bearer_json_request(requests.get, f'/Customers/{customerid}/locations/{locationid}/nodes')['nodes']

    # also worth looking at
    # https://piranha-gamma.prod.us-west-2.aws.plumenet.io/api/Customers/642cc40d71d99d000a611549/locations/642cc40d71d99d000a61154b/appFacade/home?client_id=0oa16a2cw1IIfsm7N357
    # returns nodes, devices, and statuses

    def get_nodes_by_customer(self, customer):
        """Returns a list of nodes, each with the following keys:
        connectionState ('connected')
        mac
        ethernetMac
        ip
        ipv6 (list)
        wanIp
        publicIp
        backhaulType ('ethernet')
        connectedDeviceCount
        connectionStateChangeAt
        networkMode ('router')
        leafToRoot (list; empty if node is the root)
        bootAt ('2024-02-07T10:36:31.000Z')
        alerts (list)
        speedTest {
          startedAt
          gateway
          status ('succeeded')
          trigger ('scheduled')
          serverIp
          serverHost
          serverId
          serverName ('Pigs Can Fly Labs LLC')
          isp ('Next Level Networks')
          download
          upload
          rtt
        }
        linkStates [
          {
            ifName ('eth0')
            duplex ('full')
            linkSpeed (1000)
            isUplink (Boolean)
            hasEthClient (Boolean)
          }
        ]
        vendor {
          name ('Plume')
          partNumber ('PP403Z')
        }
        ethernetLan {
          default {
            mode ('auto')
          }
        }
        health {
          status ('excellent')
          score (5)
          details
        }
        """
        xss = [self.get_nodes_by_customerid(customer['id'], location['id'])
               for location in customer['locations']]
        return [x for xs in xss for x in xs]  # flatten

    def get_locations_by_customerid(self, customerid):
        return self.bearer_json_request(requests.get, f'/Customers/{customerid}/locations')

    def get_customers(self, offset=None):
        filter = '{"offset":' + str(offset) + ',"limit":500}'
        return self.bearer_json_request(requests.get, f'/Groups/{self.partnerid}/customers{"" if offset is None else "?filter={filter}"}')


def main(argv):
    assert len(argv) == 2, argv
    config = yaml.safe_load(open(argv[1]))
    fline = FrontlineClient(config)
    nodes_by_lid = {}
    print('reading: ', end='')
    custs = fline.get_customers()
    for customer in custs:
        for location in fline.get_locations_by_customerid(customer['id']):
            nodes_by_lid[location['id']] = fline.get_nodes_by_customerid(customer['id'], location['id'])
        print('.', end='', flush=True)
    print(' done')
    import pdb; pdb.set_trace()


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))

####

# opensync_customer_info{customerid, accountid, name, email, emailverified}
# opensync_customer_first_login_ts{customerid}
# opensync_customer_terms_ts{customerid, documentid}
# opensync_customer_privacy_ts{customerid, documentid}

# opensync_node_info{customerid, locationid, nodeid, nickname, defaultName, backhaulType, networkMode, vendorName, vendorPart, serialNumber, openSyncVersion, firmwareVersion, caplte, capdisableports, capcaptiveportalV2}
# opensync_node_towardsroot{nodeid, towardsroot}
# opensync_node_ethernet_lan_mode{nodeid, isdefault, isauto}

# opensync_node_claimed_ts{nodeid}
# opensync_node_boot_ts{nodeid}

# opensync_node_connection_state{nodeid, state}
# opensync_node_connection_state_ts{nodeid}

# opensync_node_ifSpeed{nodeid, ifName, mac, duplex, isUplink, hasEthClient}
## UPPERCASE the mac to match sonic

# opensync_node_health_score{nodeid, status}
# opensync_node_alerts{nodeid}

# opensync_node_connected_device_count{nodeid}

# opensync_node_speedtest_lesser{nodeid, gateway, status, trigger, testType, serverip}
# opensync_node_speedtest_download{nodeid}
# opensync_node_speedtest_upload{nodeid}
