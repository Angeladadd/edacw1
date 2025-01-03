#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import subprocess
import sys

def run(command):
    return subprocess.run(command, capture_output=True, encoding='UTF-8')

def get_terraform_output(output_name):
    command = ["terraform", "output", "--json", output_name]
    result = run(command)
    return json.loads(result.stdout)

def get_condenser_url(hostname):
    return f"https://{hostname}.comp0235.condenser.arc.ucl.ac.uk"

def generate_inventory():
    host_vars = {}
    workers = []
    storagenode, hostnode = None, None
    for vm in ["host", "worker", "storage"]:
        ip_data = get_terraform_output(f"{vm}_vm_ips")
        for node in ip_data:
            host_vars[node] = { "ip": [node] }
            if vm == "worker":
                workers.append(node)
            elif vm == "storage":
                storagenode = node
            elif vm == "host":
                hostnode = node

    jd = {
        "_meta": {
            "hostvars": host_vars
        },
        "all": {
            "children": ["hostnode", "storagenode", "workers"],
            "vars": {
                "grafana_url": get_condenser_url(get_terraform_output("grafana_hostname")[0]),
                "prometheus_url": get_condenser_url(get_terraform_output("prometheus_hostname")[0]),
                "s3_url": get_condenser_url(get_terraform_output("s3_hostname")[0]),
                "yarn_url": get_condenser_url(get_terraform_output("yarn_hostname")[0]),
                "sparkhistory_url": get_condenser_url(get_terraform_output("sparkhistory_hostname")[0]),
            }
        },
        "workers": { "hosts": workers },
        "hostnode": { "hosts": [hostnode] },
        "storagenode": { "hosts": [storagenode] },
    }
    return json.dumps(jd, indent=4)

if __name__ == "__main__":
    sys.stdout.write(str(generate_inventory()))
    sys.stdout.flush()
