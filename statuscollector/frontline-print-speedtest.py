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
    
    gateway_count = 0
    non_gateway_count = 0
    server_host_counts = {}
    
    custs = fline.get_customers()
    for customer in custs:
        account_id = customer.get('accountId', '')
        for location in fline.get_locations_by_customerid(customer['id']):
            nodes = fline.get_nodes_by_customerid(customer['id'], location['id'])
            for node in nodes:
                st = node.get('speedTest')
                if st:
                    is_gateway = node.get('networkMode') == 'router' or ('speedTest' in node and node['speedTest'].get('gateway') == True)
                    if is_gateway:
                        gateway_count += 1
                    else:
                        non_gateway_count += 1
                    
                    server_host = st.get('serverHost', 'Unknown')
                    server_host_counts[server_host] = server_host_counts.get(server_host, 0) + 1
                        
                    print(json.dumps({
                        'nickname': node.get('nickname'),
                        'location': location.get('name'),
                        'is_gateway': is_gateway,
                        'num_speedtests': 1,
                        'speedtest': st
                    }))
    
    print(f"Total Gateway Speedtests: {gateway_count}")
    print(f"Total Non-Gateway Speedtests: {non_gateway_count}")
    print("Tests by Server Host:")
    for host, count in sorted(server_host_counts.items(), key=lambda item: item[1], reverse=True):
        print(f"  {host}: {count}")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
