#!/usr/bin/env python3
import argparse
import sys
import yaml
import json

from frontline import FrontlineClient

def main(argv):
    parser = argparse.ArgumentParser(
        description='Print all speedtest results from Frontline API'
    )
    parser.add_argument('config', type=str, help='yaml configuration file')
    args = parser.parse_args(argv[1:])

    with open(args.config) as f:
        config = yaml.safe_load(f)

    fline = FrontlineClient(config)
    
    custs = fline.get_customers()
    for customer in custs:
        account_id = customer.get('accountId', '')
        for location in fline.get_locations_by_customerid(customer['id']):
            nodes = fline.get_nodes_by_customerid(customer['id'], location['id'])
            for node in nodes:
                st = node.get('speedTest')
                if st:
                    print(json.dumps({
                        'nickname': node.get('nickname'),
                        'location': location.get('name'),
                        'is_gateway': node.get('networkMode') == 'router' or ('speedTest' in node and node['speedTest'].get('gateway') == True),
                        'num_speedtests': 1,
                        'speedtest': st
                    }))

if __name__ == '__main__':
    sys.exit(main(sys.argv))
