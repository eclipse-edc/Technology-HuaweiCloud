output "database-ip" {
  value = huaweicloud_vpc_eip.shared.address
}

# this is needed by the CI job to obtain the PG Connection String
output "pg_connection_string" {
  value = "jdbc:gaussdb://${huaweicloud_vpc_eip.shared.address}:8000/postgres?user=root&password=${var.instance_password}&sslMode=require"
}