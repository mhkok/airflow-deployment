module "sg" {
  source  = "terraform-aws-modules/security-group/aws//modules/redshift"
  version = "~> 3.0"

  name   = "dwh-redshift"
  vpc_id = "vpc-d0f07fb4"

  # Allow ingress rules to be accessed only within current VPC
  ingress_cidr_blocks = var.ingress_cidr_blocks

  # Allow all rules for all protocols
  egress_rules = ["all-all"]
}

module "redshift" {
  source  = "terraform-aws-modules/redshift/aws"
  version = "~> 2.0"

  cluster_identifier      = "dwh-cluster"
  cluster_node_type       = "dc2.large"
  cluster_number_of_nodes = 1

  cluster_database_name   = "dwh"
  cluster_master_username = var.cluster_master_username
  cluster_master_password = var.cluster_master_password

  publicly_accessible = true

  # Group parameters
  wlm_json_configuration = "[{\"query_concurrency\": 5}]"

  # DB Subnet Group Inputs
  subnets = ["subnet-22a1097a", "subnet-fc43d88a", "subnet-a6412fc2"]
  vpc_security_group_ids = [module.sg.this_security_group_id]

  # IAM Roles
  cluster_iam_roles = [aws_iam_role.redshift-iam-role.arn]
}

# module "airflow" {
#   source  = "PowerDataHub/airflow/aws"
#   version = "0.13.0"
#   cluster_name = "airflow-dev"
#   key_name = "mhkok.aws.personal"
#   db_password = "test"
#   fernet_key = "test"
#   # insert the 9 required variables here
# }