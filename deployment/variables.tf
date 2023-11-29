variable "region" {
  default = "ap-southeast-1"
}

variable "instance_name" {
  default = "gaussdb-paul"
}
variable "instance_password" {
  default = "dJyqo4vLoL!DGFZRLPVu*DbX"
}
# this is the ID for the "vpc-default" VPC
variable "vpc_id" {
  default = "08ebdc09-de15-4c41-91cf-db1cca8c1033"
}
# this is the ID for the "subnet-default"
variable "subnet_network_id" {
  default = "c9a10db0-1fe9-4863-a740-3cd342e8b909"
}
# this is the ID for the "default" security group
variable "security_group_id" {
  default = "0599125e-1015-4628-8775-9187fd0e60e7"
}

# name of the bandwidth resources
variable "bandwidth_name" {
  default = "test-gaussdb-bandwidth"
}