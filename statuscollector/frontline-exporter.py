#!/usr/bin/env python3
# frontline-exporter.py - read subscriber state from Plume Frontline

import logging, threading, time, yaml
from collections import namedtuple
from datetime import datetime
from enum import Enum

import botocore, boto3, prometheus_client, requests.exceptions

from frontline import FrontlineClient
from exporter import ModelGauge, EmailSender


LOGGER = logging.getLogger('statuscollector.frontline-exporter')

REQUEST_TIME = prometheus_client.Summary('frontline_location_request_seconds',
                                         'Time spent waiting for location request')
REQUEST_NODE_TIME = prometheus_client.Summary('frontline_node_request_seconds',
                                         'Time spent waiting for node request')
EMAIL_SUCCESS = prometheus_client.Gauge('frontline_email_success_count',
                                        'number of emails sent successfully', ['organization'])
EMAIL_ERRORS = prometheus_client.Gauge('frontline_email_error_count',
                                       'number of email sending errors', ['organization'])


class FrontlineGauge:
    def __init__(self, metric, metric_desc, labelmap, model, value_selector):
        """
        Model is a function of zero arguments that returns a map from id
        to dictionary.
        """
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
        self.gauge = ModelGauge(metric, metric_desc, labelmap, 'id', model, _selector)
        self.update()

    def update(self):
        self.gauge.update()


class PrometheusWrapper:
    # we update customer and location info once per day
    MIN_UPDATE_INTERVAL = 24 * 60 * 60  # seconds
    MIN_NODE_UPDATE_INTERVAL = 40  # seconds

    # per metrics query, we allow this many seconds of node updates
    NODE_UPDATE_INTERVAL = 10
                            
    def __init__(self, config, emailday, emailhour):
        self.config, self.emailday, self.emailhour = config, emailday, emailhour
        self.frontline = FrontlineClient(self.config)
        
        self.last_location_update, self.last_node_update, self.last_email = 0, 0, 0
        self.errors = 0
        self.lock = threading.Lock()
        self.emailer = EmailSender(self.config)
        self.id2customer_map = {}
        self.id2location_map = {}
        self.id2node_map = {}
        self.gauges = []
        self.nodegauges = []
        self._maybe_refresh()

        labelmap = { k: k for k in ['name', 'locked', 'acceptLanguage', 'email'] }
        labelmap['id'] = 'custid'
        labelmap['accountId'] = 'nlid'
        def _custmodel():
            self._maybe_refresh()
            return self.id2customer_map
        self.gauges.append(FrontlineGauge('frontline_customer_email_verified', '1 if email verified, 0 otherwise', labelmap, _custmodel, lambda model_dict: model_dict.get('emailVerified', 0)))
        self.gauges.append(FrontlineGauge('frontline_customer_created_ts', 'When the customer was created', { 'id': 'custid', 'accountId': 'nlid' }, _custmodel, lambda model_dict: model_dict['createdAt']))
        self.gauges.append(FrontlineGauge('frontline_customer_first_login_ts', 'When the customer first logged in successfully, 0=never', { 'id': 'custid', 'accountId': 'nlid' }, _custmodel, lambda model_dict: model_dict.get('firstKnownLoginTimestamp', 0)))

        nodelabelmap = { k: k for k in ['id', 'nlid', 'custid', 'locid', 'model', 'mac', 'ethernet1Mac', 'serialNumber', 'shipDate', 'partNumber', 'firmwareVersion', 'nickname', 'backhaulType', 'ip', 'wanIp', 'publicIp', 'openSyncVersion'] }
        def _nodemodel():
            self._maybe_refresh()
            return self.id2node_map
        self.nodegauges.append(FrontlineGauge('frontline_node_info', 'Node informational labels', nodelabelmap, _nodemodel, lambda d: 1))
        identmap = { 'id': 'id', 'nlid': 'nlid' }
        self.nodegauges.append(FrontlineGauge('frontline_node_health', 'Health score of the node, -1=not connected', identmap, _nodemodel, lambda d: -1 if d['connectionState'] != 'connected' else d.get('health', {}).get('score', -2)))
        self.nodegauges.append(FrontlineGauge('frontline_node_is_bridge', '1 iff node is in bridge mode', identmap, _nodemodel, lambda d: d.get('networkMode', '') == 'bridge'))
        self.nodegauges.append(FrontlineGauge('frontline_node_connected_devices', 'Count of devices connected to the node', identmap, _nodemodel, lambda d: d.get('connectedDeviceCount', -1)))
        self.nodegauges.append(FrontlineGauge('frontline_node_connectivity_change_ts', 'Timestamp at which connection state changed', identmap, _nodemodel, lambda d: d.get('connectionStateChangeAt', -1)))
        self.nodegauges.append(FrontlineGauge('frontline_node_boot_ts', 'Timestamp at which node booted', identmap, _nodemodel, lambda d: d.get('bootAt', -1)))
        self.nodegauges.append(FrontlineGauge('frontline_node_claim_ts', 'Timestamp at which node was claimed', identmap, _nodemodel, lambda d: d['claimedAt']))
        def _linkmodel():
            self._maybe_refresh()
            return { f'{node["id"]}-{link["ifName"]}': dict(link, id=f'{node["id"]}-{link["ifName"]}', nlid=node['nlid'], nodeid=node['id']) for node in self.id2node_map.values() if 'linkStates' in node for link in node.get('linkStates', []) }
        linkmap = { k: k for k in ['nlid', 'ifName', 'duplex', 'isUplink', 'hasEthClient'] }
        linkmap['nodeid'] = 'id'
        self.nodegauges.append(FrontlineGauge('frontline_node_link_speed', 'Speed of link, 65535=no link', linkmap, _linkmodel, lambda d: d['linkSpeed']))
        def _radiofallback(d):
            channel = d['leafToRoot'][0].get('channel', d.get('backhaulChannel'))
            if channel is not None:
                for band in ['2g', '5gu', '5gl', '5g', '6g']:
                    if channel == d.get(f'{band}Channel'):
                        return band.upper()
                return 'unknown_band'
            return 'unknown_channel'
        def _parentmodel():
            self._maybe_refresh()
            return { nodeid: dict(node, radio=node['leafToRoot'][0].get('radio', _radiofallback(node)), parentId=node['leafToRoot'][0]['id']) for (nodeid, node) in self.id2node_map.items() if node.get('leafToRoot') }
        parentmap = { k: k for k in ['id', 'nlid', 'radio', 'parentId'] }
        def _channelselector(d):
            if d.get('backhaulType', '') != 'wifi':
                return -1
            channel = d['leafToRoot'][0].get('channel')
            if channel is not None:
                return channel
            return d.get('backhaulChannel', -99)
        self.nodegauges.append(FrontlineGauge('frontline_node_parent_wifi_channel', '-1 iff link to parent node is not wifi', parentmap, _parentmodel, _channelselector))
        def _speedmodel():
            self._maybe_refresh()
            return { node['id']: dict(node['speedTest'], id=node['id'], nlid=node['nlid']) for node in self.id2node_map.values() if node.get('speedTest') }
        speedmap = { 'id': 'id', 'nlid': 'nlid' }
        self.nodegauges.append(FrontlineGauge('frontline_node_speedtest_rtt', 'RTT of speedtest', speedmap, _speedmodel, lambda d: -1 if d['status'] != 'succeeded' else d['rtt']))
        self.nodegauges.append(FrontlineGauge('frontline_node_upload_mbps', 'Upload speed of speedtest', speedmap, _speedmodel, lambda d: -1 if d['status'] != 'succeeded' else d['upload']))
        self.nodegauges.append(FrontlineGauge('frontline_node_download_mbps', 'Download speed of speedtest', speedmap, _speedmodel, lambda d: -1 if d['status'] != 'succeeded' else d['download']))
        speedmap.update({ k: k for k in ['trigger', 'gateway', 'serverIp', 'serverHost', 'serverId' ] })
        self.nodegauges.append(FrontlineGauge('frontline_node_speedtest_start_ts', 'Start time for most recent speedtest', speedmap, _speedmodel, lambda d: d['startedAt']))

        def _nlichannel(node, stat):
            lowerc = stat['freqBand'].lower()
            channel = node.get(f'{lowerc}Channel')
            if channel is not None:
                return channel
            if stat['freqBand'] == '2.4G':
                return node['2gChannel']
            LOGGER.info(f'unknown freqBand {stat["freqBand"]} for node {node}')
            return -999
        def _channelmodel():
            self._maybe_refresh()
            return { f'{node["id"]}-{stat["freqBand"]}': dict(node, id=f'{node["id"]}-{stat["freqBand"]}', nodeid=node['id'], freqBand=stat['freqBand'], channelWidth=stat['channelWidth'], numPunctured=len(stat['puncturedChannels']), nlichannel=_nlichannel(node, stat)) for node in self.id2node_map.values() if 'radioStats' in node for stat in node.get('radioStats', []) }
        channelmap = { k: k for k in ['nlid', 'freqBand', 'channelWidth'] }
        channelmap['nodeid'] = 'id'
        self.nodegauges.append(FrontlineGauge('frontline_node_channel', 'Channel in use for each frequency band', channelmap, _channelmodel, lambda d: d['nlichannel']))

        # frontline_node_channel: nodeid, nlid, radioStats['freqBand'], 'channelWidth', len('puncturedChannels') has value 2gChannel, 5guChannel, 5glChannel depending on freqBand
        # whether an alert is being shown
        # customers showing weirdness in number of optimization events.

    def _maybe_refresh(self):
        oldtime = -1
        newtime = time.time()
        with self.lock:
            if time.time() - self.last_location_update > self.MIN_UPDATE_INTERVAL:
                oldtime = self.last_location_update
                self.last_location_update = newtime
            # safe to release the lock here because (we assume) that the
            # min update interval is long enough that this method will exit
            # before any other thread has oldtime != -1.
        if oldtime != -1:
            try:
                self._refresh()
                for g in self.gauges:
                    g.update()
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError):
                LOGGER.exception()
                self.errors += 1
                # reset the last update time so we try again pronto
                with self.lock:
                    if self.last_location_update == newtime:
                        self.last_location_update = oldtime
        with self.lock:
            do_update = False
            if time.time() - self.last_node_update > self.MIN_NODE_UPDATE_INTERVAL:
                try:
                    self._refresh_some_nodes_locked()
                    self.last_node_update = time.time()
                except (requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError):
                    LOGGER.exception()
                    self.errors += 1
                do_update = True
            # safe to release the lock here because g.update() is thread-safe
        if do_update:
            for g in self.nodegauges:
                g.update()
        with self.lock:
            now = datetime.utcnow()
            if (self.emailday == now.weekday() and self.emailhour <= now.hour
                and time.time() - self.last_email > 3600*12):
                try:
                    self._send_email()
                    self.last_email = time.time()
                except botocore.exceptions.ClientError:
                    EMAIL_ERRORS.labels(organization='UNKNOWN').inc()
                    LOGGER.exception()

    def _send_email(self):
        # see https://codelovingyogi.medium.com/sending-emails-using-aws-simple-email-service-ses-220de9db4fc8
        for (name, d) in self.config['organizations'].items():
            report_to = d.get('pastdue_report_to')
            if report_to:
                if isinstance(report_to, list):
                    dests = report_to
                elif isinstance(report_to, str):
                    dests = [report_to]
                else:
                    assert False, f'pastdue_report_to { report_to } for { name } has bad type'
                subject = 'FIXME'
                lineend = '\n   '
                body = 'FIXME'
                response = self.emailer.send(
                    'support@nextlevel.net',
                    subject,
                    body,
                    to=dests,
                    cc=['accounting@nextlevel.net']
                    )
                if 'Error' in response:
                    EMAIL_ERRORS.labels(organization=name).inc()
                    LOGGER.error(f'failed to send email to { ", ".join(dests) }: { response["Error"] }')
                else:
                    EMAIL_SUCCESS.labels(organization=name).inc()
                    LOGGER.info(f'sent email to { ", ".join(dests) }')

    @REQUEST_TIME.time()
    def _refresh(self):
        LOGGER.info('refreshing Frontline customers and locations')
        self.id2customer_map = { cust['id']: cust for cust in self.frontline.get_customers() }
        self.id2location_map = { loc['id']: dict(loc, custid=cust['id']) for cust in self.id2customer_map.values() for loc in self.frontline.get_locations_by_customerid(cust['id']) }
        LOGGER.info('refresh complete')

    @REQUEST_NODE_TIME.time()
    def _refresh_some_nodes_locked(self):
        def _upper(d, k):
            v = d.get(k)
            if v:
                d[k] = v.upper()
        def _makedict(loc, node):
            # sonic_stats uses uppercase MAC addresses so we should too,
            # in order to join on them in Prometheus
            _upper(node, 'mac')
            _upper(node, 'ethernet1Mac')
            return dict(node, nlid=self.id2customer_map[loc['custid']]['accountId'], custid=loc['custid'], locid=loc['id'])
        if self.id2node_map:
            start = time.time()
            updated = 0
            while time.time() - start < self.NODE_UPDATE_INTERVAL:
                if self.next_location_to_update >= len(self.locations_to_update):
                    self.next_location_to_update = 0
                    LOGGER.info('updated nodes at all known locations; starting over')
                locid = self.locations_to_update[self.next_location_to_update]
                self.next_location_to_update += 1
                loc = self.id2location_map[locid]
                self.id2node_map.update({ node['id']: _makedict(loc, node) for node in self.frontline.get_nodes_by_customerid(loc['custid'], loc['id']) })
                updated += 1
        else:
            # the first time, we grab them all
            LOGGER.info('refreshing all Frontline nodes')
            self.id2node_map = { node['id']: _makedict(loc, node) for loc in self.id2location_map.values() for node in self.frontline.get_nodes_by_customerid(loc['custid'], loc['id']) }
            LOGGER.info('refresh complete')
            self.locations_to_update = sorted(self.id2location_map.keys())
            self.next_location_to_update = 0


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
    parser.add_argument('--emailday', type=int, default=-1, help='UTC day of week on which to send summary email (-1=none, 0=Monday)')
    parser.add_argument('--emailhour', type=int, default=14, help='UTC hour in day to send summary email')
    vals = parser.parse_args(args=args[1:])

    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    LOGGER.info(f'main() {" ".join(args)}')

    config = yaml.safe_load(open(vals.config))

    if vals.port:
        if vals.emailday < 0 or vals.emailday > 6:
            LOGGER.info('--emailday does not specify a valid day: no weekly summary email will be sent')
        else:
            LOGGER.info('will send weekly email subscriber summaries')
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
