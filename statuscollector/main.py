#!/usr/bin/python3

import datetime, yaml

from uisp import UispClient, Organizations, ServiceStatus, print_all_clients, currency_str
from observium import ObserviumClient


class IdMapper:
    """Creates a map from an ID to a list of objects having that ID."""
    def __init__(self, services, idattribute):
        self.idmap = {}
        for s in services:
            spid = s[idattribute]
            sofar = self.idmap.get(spid, [])
            if not sofar:
                self.idmap[spid] = sofar
            sofar.append(s)


def main(argv):
    config = yaml.safe_load(open(argv[1]))
    organizations = Organizations(config)
    uisp = UispClient(config)

    overall_receivable, overall_credit = 0, 0
    orgs = uisp.get_organizations()
    for org in orgs:
        clients = uisp.get_clients_of(org)
        active_clients = [c for c in clients if c['isActive'] and not c['isArchived'] and not c['isLead']]
        archived_clients = [c for c in clients if c['isArchived']]
        archived = f' ({len(archived_clients)} archived)' if archived_clients else ''
        payables = [c['accountBalance'] for c in clients if c['accountBalance'] > 0]
        overall_credit += sum(payables)
        credit = f', {len(payables)} clients have total credit {currency_str(sum(payables))}' if payables else ''
        receivables = [c['accountBalance'] for c in clients if c['accountBalance'] < 0]
        overall_receivable += sum(receivables)
        services = uisp.get_services_of(org)
        active_services = [s for s in services if s['status'] == ServiceStatus.ACTIVE.value]
        today = datetime.date.today()
        lastmonth = (today - datetime.timedelta(days=14+today.day)).isoformat()
        ended_services = [s for s in services if s['status'] == ServiceStatus.ENDED.value and (s['activeTo'] or '').startswith(lastmonth[0:8])]

        clientmap = IdMapper(clients, 'id')
        this_month = IdMapper(active_services, 'servicePlanId')
        last_month = IdMapper(ended_services, 'servicePlanId')
        last_month_cids = IdMapper(ended_services, 'clientId')

        # for each service plan with at least one active service:
        #   * show the clients who are active on that service plan,
        #   * warn if the count of actives is below target,

        print(f'\n{org["name"]}: {len(active_clients)} active of {len(clients)} clients{archived}')
        # \n{len(receivables)} clients owe total {currency_str(-sum(receivables))}{credit}:\n')
        cids_with_service = set()
        nli_capitated_nonconnectivity = 0
        nli_capitated_connectivity = 0
        revenue_after_nli_capitated = 0
        values = {}
        for (spid, services) in this_month.idmap.items():
            for s in services:
                organizations.register_service(s)
            owner = organizations.get_owner(spid)
            values = values or owner.values
            nli_capitated_nonconnectivity += owner.total_capitated_to_nli()
            nli_capitated_connectivity += owner.total_capitated_connectivity()
            revenue_after_nli_capitated += owner.remainder_after_nli_capitation()
            warning = f' (WARNING less than target {owner.target_actives})' if owner.target_actives > owner.active_services else f' (target {owner.target_actives})'
            nli100 = '' if owner.values else f' 100% NLI' 
            dls = services[0]['downloadSpeed']
            speed = f' {int(dls)} Mbps' if dls else ''
            print(f'\n=== {services[0]["name"]}({spid}){speed} has {owner.active_services} actives{warning}{nli100}')
            cids = { s['clientId'] for s in services  }
            cids_with_service |= cids
            cids_with_new_service = { s['clientId'] for s in services if s['activeFrom'] >= today.isoformat()[0:8] }

            # ordinarily we print only clients who have service. if you also
            # want to print clients without service, uncomment the next line.
            #print_all_clients(clients, uisp, cids_with_new_service)
            print_all_clients([clientmap.idmap[cid][0] for cid in cids], uisp, cids_with_new_service)

        # A client was dropped from previous month to current month if that
        # client is in previous month drops but not in current month actives.

        dropped_clients = { s['clientId'] for a in last_month.idmap.values() for s in a } - { s['clientId'] for a in this_month.idmap.values() for s in a }
        for c in clients:
            if c['id'] in dropped_clients:
                print(f'**** {uisp.name_of(c)} no longer has service')
            if c['id'] in cids_with_service and not c['username']:
                LOGGER.warning(f'**** WARNING: client has no username: {uisp.name_of(c)}')

        fmp = values.get('fixed_monthly_payouts', [])
        fmpstr = (', ' + ', '.join(f'{p[0]} {currency_str(p[1])}' for p in fmp)) if fmp else ''
        nli_capped_connectivity = min(max(nli_capitated_connectivity, values.get('capitated_connectivity_min', 0)), values.get('capitated_connectivity_max', 100000000))
        monthly = values.get('nli_monthly_connectivity', 0)
        net = revenue_after_nli_capitated - monthly - nli_capped_connectivity - sum([p[1] for p in fmp])
        print(f'\n === NLI capitated nonconnectivity {currency_str(nli_capitated_nonconnectivity)}, NLI connectivity {currency_str(monthly + nli_capped_connectivity)}{fmpstr}, net to NLI {currency_str(nli_capitated_nonconnectivity + monthly + nli_capped_connectivity)}, net to customer {currency_str(net)}')

    print(f'\ngrand total receivable: {currency_str(-overall_receivable)}, grand total credit: {currency_str(overall_credit)}, net: {currency_str(-overall_receivable-overall_credit)}')


if __name__ == '__main__':
    import sys
    assert len(sys.argv) == 2, sys.argv
    sys.exit(main(sys.argv))
