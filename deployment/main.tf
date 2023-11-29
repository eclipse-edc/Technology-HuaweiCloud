terraform {
  required_providers {
    huaweicloud = {
      source  = "huaweicloud/huaweicloud"
      version = "1.57.0"
    }
  }
}

provider "huaweicloud" {
  region = var.region
}
