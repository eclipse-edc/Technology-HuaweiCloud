data "huaweicloud_availability_zones" "test" {
}

resource "huaweicloud_gaussdb_opengauss_instance" "instance_acc" {
  vpc_id            = var.vpc_id
  subnet_id         = var.subnet_network_id
  security_group_id = var.security_group_id
  name              = var.instance_name
  password          = var.instance_password
  flavor            = "gaussdb.opengauss.ee.m6.2xlarge.x868.ha"
  availability_zone = join(",", slice(data.huaweicloud_availability_zones.test.names, 0, 3))

  replica_num  = 3
  sharding_num = 1

  ha {
    mode             = "centralization_standard"
    replication_mode = "sync"
    consistency      = "eventual"
  }

  volume {
    type = "ULTRAHIGH"
    size = 40
  }
}


resource "huaweicloud_vpc_bandwidth" "gaussdb-bw-shared" {
  name = var.bandwidth_name
  size = 5
}

resource "huaweicloud_vpc_eip" "node1" {
  publicip {
    # can be 5_bgp (dynamic BGP) and 5_sbgp (static BGP)
    type = "5_bgp"
  }

  bandwidth {
    # WHOLE means shared bandwidth
    share_type = "WHOLE"
    id         = huaweicloud_vpc_bandwidth.gaussdb-bw-shared.id
  }
}
resource "huaweicloud_vpc_eip" "node2" {
  publicip {
    # can be 5_bgp (dynamic BGP) and 5_sbgp (static BGP)
    type = "5_bgp"
  }

  bandwidth {
    # WHOLE means shared bandwidth
    share_type = "WHOLE"
    id         = huaweicloud_vpc_bandwidth.gaussdb-bw-shared.id
  }
}

resource "huaweicloud_vpc_eip" "node3" {
  publicip {
    # can be 5_bgp (dynamic BGP) and 5_sbgp (static BGP)
    type = "5_bgp"
  }

  bandwidth {
    # WHOLE means shared bandwidth
    share_type = "WHOLE"
    id         = huaweicloud_vpc_bandwidth.gaussdb-bw-shared.id
  }
}


resource "huaweicloud_vpc_eip_associate" "assoc1" {
  public_ip  = huaweicloud_vpc_eip.node1.address
  network_id = var.subnet_network_id
  fixed_ip   = huaweicloud_gaussdb_opengauss_instance.instance_acc.private_ips[0]
}

resource "huaweicloud_vpc_eip_associate" "assoc2" {
  public_ip  = huaweicloud_vpc_eip.node2.address
  network_id = var.subnet_network_id
  fixed_ip   = huaweicloud_gaussdb_opengauss_instance.instance_acc.private_ips[1]
}
resource "huaweicloud_vpc_eip_associate" "assoc3" {
  public_ip  = huaweicloud_vpc_eip.node3.address
  network_id = var.subnet_network_id
  fixed_ip   = huaweicloud_gaussdb_opengauss_instance.instance_acc.private_ips[2]
}