data "harvester_image" "img" {
  display_name = var.img_display_name
  namespace    = "harvester-public"
}

data "harvester_ssh_key" "mysshkey" {
  name      = var.keyname
  namespace = var.namespace
}

resource "random_id" "secret" {
  byte_length = 5
}

resource "harvester_cloudinit_secret" "cloud-config" {
  name      = "cloud-config-${random_id.secret.hex}"
  namespace = var.namespace

  user_data = templatefile("cloud-init.tmpl.yml", {
      public_key_openssh = data.harvester_ssh_key.mysshkey.public_key
    })
}

resource "harvester_virtualmachine" "hostvm" {
  
  count = 1

  name                 = "${var.username}-host-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  namespace            = var.namespace
  restart_after_update = true

  description = "Cluster Host Node"

  cpu    = 2
  memory = "4Gi"

  efi         = true
  secure_boot = false

  run_strategy    = "RerunOnFailure"
  hostname        = "${var.username}-host-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  reserved_memory = "100Mi"
  machine_type    = "q35"

  network_interface {
    name           = "nic-1"
    wait_for_lease = true
    type           = "bridge"
    network_name   = var.network_name
  }

  disk {
    name       = "rootdisk"
    type       = "disk"
    size       = "10Gi"
    bus        = "virtio"
    boot_order = 1

    image       = data.harvester_image.img.id
    auto_delete = true
  }

  tags = {
    condenser_ingress_isEnabled = true
    condenser_ingress_isAllowed = true
    condenser_ingress_spark_hostname = "${var.username}-spark"
    condenser_ingress_spark_port = 4040
    condenser_ingress_spark_nginx_proxy-body-size = "10000000m"
    condenser_ingress_sparkhistory_hostname = "${var.username}-sparkhistory"
    condenser_ingress_sparkhistory_port = 18080
    condenser_ingress_sparkhistory_nginx_proxy-body-size = "10000000m"
    condenser_ingress_yarn_hostname = "${var.username}-yarn"
    condenser_ingress_yarn_port = 8088
    condenser_ingress_yarn_nginx_proxy-body-size = "10000000m"
    condenser_ingress_prometheus_hostname = "${var.username}-prometheus"
    condenser_ingress_prometheus_port = 9090
    condenser_ingress_prometheus_nginx_proxy-body-size = "10000000m"
    condenser_ingress_node_hostname = "${var.username}-node"
    condenser_ingress_node_port = 9100
    condenser_ingress_node_nginx_proxy-body-size = "10000000m"
    condenser_ingress_grafana_hostname = "${var.username}-grafana"
    condenser_ingress_grafana_port = 3000
    condenser_ingress_grafana_nginx_proxy-body-size = "10000000m"
  }

  cloudinit {
    user_data_secret_name = harvester_cloudinit_secret.cloud-config.name
  }
}

resource "harvester_virtualmachine" "workervm" {
  
  count = var.worker_count

  name                 = "${var.username}-worker-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  namespace            = var.namespace
  restart_after_update = true

  description = "Cluster Compute Node"

  cpu    = 4
  memory = "32Gi"

  efi         = true
  secure_boot = false

  run_strategy    = "RerunOnFailure"
  hostname        = "${var.username}-worker-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  reserved_memory = "100Mi"
  machine_type    = "q35"

  network_interface {
    name           = "nic-1"
    wait_for_lease = true
    type           = "bridge"
    network_name   = var.network_name
  }

  disk {
    name       = "rootdisk"
    type       = "disk"
    size       = "25Gi"
    bus        = "virtio"
    boot_order = 1

    image       = data.harvester_image.img.id
    auto_delete = true
  }

  cloudinit {
    user_data_secret_name = harvester_cloudinit_secret.cloud-config.name
  }
}

resource "harvester_virtualmachine" "storagevm" {
  
  count = var.storage_count

  name                 = "${var.username}-storage-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  namespace            = var.namespace
  restart_after_update = true

  description = "Cluster Compute Node"

  cpu    = 4
  memory = "8Gi"

  efi         = true
  secure_boot = false

  run_strategy    = "RerunOnFailure"
  hostname        = "${var.username}-storage-${format("%02d", count.index + 1)}-${random_id.secret.hex}"
  reserved_memory = "100Mi"
  machine_type    = "q35"

  network_interface {
    name           = "nic-1"
    wait_for_lease = true
    type           = "bridge"
    network_name   = var.network_name
  }

  disk {
    name       = "rootdisk"
    type       = "disk"
    size       = "10Gi"
    bus        = "virtio"
    boot_order = 1

    image       = data.harvester_image.img.id
    auto_delete = true
  }

  disk {
    name       = "datadisk"
    type       = "disk"
    size       = "200Gi"
    bus        = "virtio"
    boot_order = 2

    auto_delete = true
  }

  tags = {
    condenser_ingress_isEnabled = true
    condenser_ingress_os_hostname = "${var.username}-s3"
    condenser_ingress_os_port = 9000
    condenser_ingress_os_protocol = "https"
    condenser_ingress_os_nginx_proxy-body-size = "100000m"
    condenser_ingress_cons_hostname = "${var.username}-cons"
    condenser_ingress_cons_port = 9001
    condenser_ingress_cons_protocol = "https"
    condenser_ingress_cons_nginx_proxy-body-size = "100000m"
  }

  cloudinit {
    user_data_secret_name = harvester_cloudinit_secret.cloud-config.name
  }
}