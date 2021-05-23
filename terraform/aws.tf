terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
      region  = "us-west-2"
      profile = "matthijs.kok"
    }
  }
}

provider "aws" {
    region = "us-west-2"
}