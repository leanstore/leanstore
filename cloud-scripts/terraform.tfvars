# terraform.tfvars

public_key_path = "~/.ssh/benchmark_key.pub"
region          = "us-west-2"
az              = "us-west-2a"
ami             = "ami-830c94e3"  # Dummy AMI for LocalStack

instance_types = {
  "bookkeeper" = "t2.micro"
  "zookeeper"  = "t2.micro"
  "client"     = "t2.micro"
}

num_instances = {
  "bookkeeper" = 3
  "zookeeper"  = 3
  "client"     = 2
}
