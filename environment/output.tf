output host_vm_ips {
  value = harvester_virtualmachine.hostvm[*].network_interface[0].ip_address
}

output host_vm_ids {
  value = harvester_virtualmachine.hostvm.*.id
}

output worker_vm_ips {
  value = harvester_virtualmachine.workervm[*].network_interface[0].ip_address
}

output worker_vm_ids {
  value = harvester_virtualmachine.workervm.*.id
}

output storage_vm_ips {
  value = harvester_virtualmachine.storagevm[*].network_interface[0].ip_address
}

output storage_vm_ids {
  value = harvester_virtualmachine.storagevm.*.id
}

output grafana_hostname {
  value = harvester_virtualmachine.hostvm[*].tags["condenser_ingress_grafana_hostname"]
}

output prometheus_hostname {
  value = harvester_virtualmachine.hostvm[*].tags["condenser_ingress_prometheus_hostname"]
}

output yarn_hostname {
  value = harvester_virtualmachine.hostvm[*].tags["condenser_ingress_yarn_hostname"]
}

output sparkhistory_hostname {
  value = harvester_virtualmachine.hostvm[*].tags["condenser_ingress_sparkhistory_hostname"]
}

output s3_hostname {
  value = harvester_virtualmachine.storagevm[*].tags["condenser_ingress_os_hostname"]
}

output minio_hostname {
  value = harvester_virtualmachine.storagevm[*].tags["condenser_ingress_cons_hostname"]
}

output sparklog_hostnames {
  value = harvester_virtualmachine.workervm[*].tags["condenser_ingress_sparklog_hostname"]
}