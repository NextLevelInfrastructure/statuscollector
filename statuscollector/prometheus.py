#!/usr/bin/env python3
"""
prometheus.py

The Prometheus API as documented in
https://github.com/4n4nd/prometheus-api-client-python
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

