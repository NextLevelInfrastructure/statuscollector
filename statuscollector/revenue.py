#!/usr/bin/python3

import datetime, itertools, logging, math, yaml

from uisp import UispClient, Organizations, ServiceStatus, InvoiceStatus
from waveapps import Waveapps
from main import IdMapper


LOGGER = logging.getLogger('statuscollector.revenue')


def panic(message):
    import pdb, sys
    if sys.stdout.isatty():
        LOGGER.error(message)
        pdb.set_trace()
    else:
        LOGGER.fatal(message)
        sys.exit(1)


class InvoiceSummarizer:
    def __init__(self, organizations, servicemap):
        """
        One InvoiceSummarizer is created for each organization, and servicemap
        contains only services for that organization.
        """
        self.organizations = organizations
        self.servicemap = servicemap
        self.active_service_count = sum([1 for services in servicemap.idmap.values() for s in services if s['status'] == ServiceStatus.ACTIVE.value and s['servicePlanType'] != 'General'])

    def _apply_cap(self, values, connectivity_per_service):
        total_capitated = connectivity_per_service * self.active_service_count
        capped = min(max(total_capitated, values.get('capitated_connectivity_min', 0)), values.get('capitated_connectivity_max', 100000000))
        return capped / self.active_service_count

    def summarize(self, invoice):
        """For purposes of recognition of revenue, non-capitated backhaul
        is allocated equally to each service of type Internet that is
        active on the day revenue is recognized. Fixed monthly payouts are
        not recognized as revenue; they are debited from Custodial Funds
        as they are paid out.
        """
        sofar = InvoiceSummary()
        for item in invoice['items']:
            quantity = math.ceil(round(item.get('quantity', 1), 2))
            # quantity is between 0 and 1 when a service starts or ends
            # in the middle of a month. We bill for a full management/ISP rate.
            # quantity may be over 1 if we are billing for multiple months
            # in one invoice.
            #if quantity != 1 and item['id'] != 5875:
            #    panic(f'quantity is not 1 for item {item}')
            lab = { item.get('label', 'no-label'): quantity }
            itemtotal = item.get('total')
            serviceid = item.get('serviceId')
            if serviceid:
                spid = self.servicemap.idmap[serviceid][0]['servicePlanId']
                wrapper = self.organizations.get_wrapper(spid)
                if wrapper.values:
                    thisone = InvoiceSummary(
                        lab,
                        nli_management=wrapper.management_per_service*quantity,
                        nli_isp=wrapper.isp_per_service*quantity,
                        nli_backhaul=quantity*(self._apply_cap(wrapper.values, wrapper.connectivity_per_service) + wrapper.monthly_connectivity_weight * wrapper.values.get('nli_monthly_connectivity', 0) / self.active_service_count),
                        nli_billing=(wrapper.DEFAULT_BILLING_RATE * itemtotal) if wrapper.billing_fee is None else (quantity*wrapper.billing_fee))
                    sofar.custodial_funds += itemtotal - thisone.total_value()
                    sofar = sofar.add(thisone)
                else:  # 100% NLI
                    assert itemtotal, f'complimentary items must be defined in the config file: {invoice}'
                    sofar = sofar.add(InvoiceSummary(lab, nli_other=itemtotal))
            elif item.get('productId'):
                sofar = sofar.add(InvoiceSummary(lab, products=itemtotal))
            elif item.get('type', '') == 'other':
                sofar = sofar.add(InvoiceSummary(lab, nli_other=itemtotal))
            else:
                panic(f'unknown item {item} for {invoice}')
        return sofar


class InvoiceSummary:
    def __init__(self, constituents={}, nli_management=0, nli_isp=0, nli_backhaul=0, nli_billing=0, nli_other=0, custodial_funds=0, products=0, taxes=[]):
        self.constituents = constituents
        self.nli_management = nli_management
        self.nli_isp = nli_isp
        self.nli_backhaul = nli_backhaul
        self.nli_billing = nli_billing
        self.nli_other = nli_other
        self.custodial_funds = custodial_funds
        self.products = products
        self.taxes = taxes

    def __str__(self):
        constituents = ', '.join([f'{k}: {v}' for k, v in self.constituents.items()])
        numitems = sum(self.constituents.values())
        taxsum = sum([tax['totalValue'] for tax in self.taxes])
        def cur(v):
            vv = round(v, 2)
            return '${:.2f}'.format(vv)
        return f'InvoiceSummary[total {cur(self.total_value())} from {numitems} items ({constituents}), mgmt {cur(self.nli_management)}, isp {cur(self.nli_isp)}, backhaul {cur(self.nli_backhaul)}, billing {cur(self.nli_billing)}, other {cur(self.nli_other)}, custodial {cur(self.custodial_funds)}, products {cur(self.products)}, {len(self.taxes)} taxes totaling {cur(taxsum)}]'

    def total_value(self):
        return self.nli_management + self.nli_isp + self.nli_backhaul + self.nli_billing + self.nli_other + self.custodial_funds + self.products + sum([tax['totalValue'] for tax in self.taxes])

    def is_zero(self):
        return self.nli_management == 0 and self.nli_isp == 0 and self.nli_backhaul == 0 and self.nli_billing == 0 and self.nli_other == 0 and self.custodial_funds == 0 and self.products == 0 and not self.taxes

    def add(self, other):
        newlabels = self.constituents.copy()
        for k, v in other.constituents.items():
            newlabels[k] = v + newlabels.get(k, 0)
            
        return InvoiceSummary(
            constituents=newlabels,
            nli_management=self.nli_management + other.nli_management,
            nli_isp=self.nli_isp + other.nli_isp,
            nli_backhaul=self.nli_backhaul + other.nli_backhaul,
            nli_billing=self.nli_billing + other.nli_billing,
            nli_other=self.nli_other + other.nli_other,
            custodial_funds=self.custodial_funds + other.custodial_funds,
            products=self.products + other.products,
            taxes=self.taxes + other.taxes
        )


class JournalEntry:
    def __init__(self, config, owner):
        """Owner can be None if not an organization"""
        self.wave = Waveapps(config['waveapps']['endpoint'])
        self.accountmap = config['waveapps'][owner] if owner else {} # FIXME

    def reverse_and_commit(self, day, summary):
        panic('FIXME: not implemented')
        transactionid = None
        return transactionid

    def commit(self, day, summary):
        """If self.owner is not NLI, debit Subscriber Revenue Receivable and
        credit the appropriate revenue accounts. If self.owner is NLI,
        debit Direct Customer Receivables and credit Direct Customer Revenue.
        """
        # FIXME: not implemented
        print('committing', day, summary)
        transactionid = None
        return transactionid

    def record_payments(self, day, count, value):
        """If self.owner is not NLI, debit Subscriber Receipts and credit
        Subscriber Receivables. If self.owner is NLI, credit Direct
        Customer Receivables and debit Direct Customer Receipts.
        """
        # FIXME: not implemented
        print(f'recording {count} payments on', day, round(value, 2))
        transactionid = None
        return transactionid


def main(argv):
    assert len(argv) == 2, argv
    config = yaml.safe_load(open(argv[1]))
    organizations = Organizations(config)
    uisp = UispClient(config)
    all_attribs = uisp.get_custom_attributes()
    invoice_aidmap = { a['key']: a['id'] for a in all_attribs if a['attributeType'] == 'invoice' }
    payment_aidmap = { a['key']: a['id'] for a in all_attribs if a['attributeType'] == 'payment' }
    aid_recognized_on = invoice_aidmap['recognizedOn']

    orgs = uisp.get_organizations()
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    # invoices before 2023-09-23 are historical NLN invoices
    startdate = max(yesterday - datetime.timedelta(days=90), datetime.date(2023, 9, 23))
    payments = uisp.get_payments(startdate=startdate.isoformat(),
                                 enddate=yesterday.isoformat())

    for org in orgs:
        print('===', org['name'])
        clients = uisp.get_clients_of(org)
        invoices = uisp.get_invoices_of(org, startdate=startdate.isoformat(),
                                        enddate=yesterday.isoformat())

        # FIXME: calculate active_services for the month of the invoice,
        # not just today, since capitated backhaul cost may change.
        all_services = uisp.get_services_of(org)
        active_services = [s for s in all_services if s['status'] == ServiceStatus.ACTIVE.value]
        servicemap = IdMapper(all_services, 'id')
        serviceplan_map = IdMapper(active_services, 'servicePlanId')
        owner = None
        for (spid, services) in serviceplan_map.idmap.items():
            for s in services:
                wrapper = organizations.register_service(s)
                if owner:
                    assert owner == wrapper.owner or not wrapper.owner, (owner, wrapper.owner)
                else:
                    owner = wrapper.owner
        jentry = JournalEntry(config, owner)
        summarizer = InvoiceSummarizer(organizations, servicemap)

        for day, invoices_this_day in itertools.groupby(invoices, key=lambda i: i['createdDate'].split('T')[0]):
            summary = InvoiceSummary()
            invoices_recognized = []
            for invoice in invoices_this_day:
                today = datetime.date.today().isoformat()
                attribs = { attrib['key']: attrib['value'] for attrib in invoice['attributes'] }
                assert 'recognized_by' in attribs or 'recognized_on' not in attribs, invoice
                if invoice['status'] == InvoiceStatus.VOID.value:
                    if 'recognized_by' not in attribs:
                        # then there is nothing to reverse for this invoice
                        continue
                    if 'reversed_by' in attribs:
                        assert 'VOID' in attribs.get('recognized_on', ''), invoice
                    else:
                        ron = attribs.get('recognized_on', 'VOID')
                        assert 'VOID' not in ron, invoice
                        voided = f'{ron}VOID{today}'
                        # FIXME uisp.patch_invoice_attribute(invoice['id'], aid_recognized_on, voided)
                        tid = jentry.reverse_and_commit(day, summarizer.summarize(invoice))
                        # FIXME uisp.patch_invoice_attribute(invoice['id'], invoice_aidmap['reversedBy'], tid)
                elif InvoiceStatus.may_be_paid(invoice['status']) and 'recognized_by' not in attribs:
                    assert 'recognized_on' not in attribs, invoice
                    # FIXME uisp.patch_invoice_attribute(invoice['id'], aid_recognized_on, today)
                    invoices_recognized.append(invoice)
                    summary = summary.add(summarizer.summarize(invoice))
                else:
                    panic(f'unknown invoice status for {invoice}')
            if invoices_recognized:
                assert not summary.is_zero(), (summary, invoices_recognized)
                tid = jentry.commit(day, summary)
                for invoice in invoices_recognized:
                    pass # FIXME uisp.patch_invoice_attribute(invoice['id'], invoice_aidmap['recognizedBy'], tid)
            else:
                assert summary.is_zero(), invoices_this_day

        # having processed all invoices, now process payments for this owner
        invoicemap = IdMapper(invoices, 'id')
        for day, payments_this_day in itertools.groupby(payments, key=lambda i: i['createdDate'].split('T')[0]):
            sofar = 0
            count = 0
            for payment in payments_this_day:
                for cover in payment['paymentCovers']:
                    iid = cover['invoiceId']
                    if iid:
                        ourinvoice = invoicemap.idmap.get(iid)
                        if ourinvoice is None:
                            continue  # not a payment for one of our invoices
                        sofar += cover['amount']
                        count += 1
                    else:
                        rid = cover['refundId']
                        assert rid, cover
                        # FIXME: not implemented
            if sofar > 0:
                tid = jentry.record_payments(day, count, sofar)


if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))
