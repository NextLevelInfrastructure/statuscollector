#!/usr/bin/python3

import datetime, logging


LOGGER = logging.getLogger('statuscollector.waveapps')


class Waveapps:
    def __init__(self, endpoint):
        self.gql_endpoint = endpoint
