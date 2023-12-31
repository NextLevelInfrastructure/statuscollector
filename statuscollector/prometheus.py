#!/usr/bin/env python3
"""
prometheus.py

The Prometheus API as documented in
https://github.com/4n4nd/prometheus-api-client-python

## For each non-lab core router reporting data to Prometheus,
## show its 15-minutely ingressing data rate. We should complain if
## this number is not equal to the number of non-lab core routers
## that we have.

sum by (instance)(rate(ifInOctets{cabinet!~"lab-.*",role=~"rtc.*"}[15m]))

sonic_stats_rxpower_dbm{cabinet="$cabinet"}

ifAdminStatus{ifAlias="Cust: foo 21080_road", ifIndex="3", ifName="Port3", instance="cabinet-sws02.owner"}
   ***** values 1 up, 2 down

ifOperStatus{}
   ***** values: 1 up, 2 down, others are weird

ifHighSpeed{}
   ***** values: 1000 or 10000

ifInErrors, ifInOctets, ifInDiscards
ifOutErrors, ifOutOctets
"""

import logging, prometheus_client

from prometheus_api_client import PrometheusConnect


REQUEST_TIME = prometheus_client.Summary('promapi_processing_seconds',
                                         'time of Prometheus API requests')
LOGGER = logging.getLogger('statuscollector.prometheus')


class PrometheusClientError(Exception):
    pass


class PrometheusClient:
    def __init__(self, config):
        self.config = config.get('prometheus', {})
        urlprefix = self.config.get('urlprefix')
        if not self.urlprefix:
            raise PrometheusClientError('no urlprefix in prometheus config')
        self.prom = PrometheusConnect(url=urlprefix, disable_ssl=True)
        self.timeout = self.config.get('timeout', 10)

    def get_devices(self):
        # see https://github.com/4n4nd/prometheus-api-client-python
        pass

