#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import subprocess
import sys

def run(command):
    return subprocess.run(command, capture_output=True, encoding='UTF-8')

def generate_inventory():
    host_vars = {}
    workers = []
    storagenode, hostnode = None, None
    for vm in ["host", "worker", "storage"]:
        command = (f"terraform output --json {vm}_vm_ips").split()
        ip_data = json.loads(run(command).stdout)
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
        "all": { "children": ["hostnode", "storagenode", "workers"] },
        "workers": { "hosts": workers },
        "hostnode": { "hosts": [hostnode] },
        "storagenode": { "hosts": [storagenode] }
    }
    return json.dumps(jd, indent=4)


if __name__ == "__main__":
    sys.stdout.write(str(generate_inventory()))
    sys.stdout.flush()
