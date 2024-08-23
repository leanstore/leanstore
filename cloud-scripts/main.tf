terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.16"
    }
  }

  required_version = ">= 1.2.0"
}

resource "random_id" "hash" {
  byte_length = 8
}

variable "key_name" {
  default     = "benchmark-key"
  description = "Desired name prefix for the AWS key pair"
}

variable "public_key_path" {}
variable "region" {}
variable "az" {}
variable "ami" {}
variable "instance_types" {}
variable "num_instances" {}

provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = var.region

  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    ec2 = "http://localhost:4566"
  }
}


# Create a VPC to launch our instances into
resource "aws_vpc" "benchmark_vpc" {
  cidr_block = "10.0.0.0/16"

  tags = {
    Name = "Bookkeeper-Benchmark-VPC-${random_id.hash.hex}"
  }
}

# Create an internet gateway to give our subnet access to the outside world
resource "aws_internet_gateway" "bookkeeper" {
  vpc_id = aws_vpc.benchmark_vpc.id
}

# Grant the VPC internet access on its main route table
resource "aws_route" "internet_access" {
  route_table_id         = aws_vpc.benchmark_vpc.main_route_table_id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.bookkeeper.id
}

# Create a subnet to launch our instances into
resource "aws_subnet" "benchmark_subnet" {
  vpc_id                  = aws_vpc.benchmark_vpc.id
  cidr_block              = "10.0.0.0/24"
  map_public_ip_on_launch = true
  availability_zone       = var.az
}

resource "aws_security_group" "benchmark_security_group" {
  name   = "terraform-bookkeeper-${random_id.hash.hex}"
  vpc_id = aws_vpc.benchmark_vpc.id

  # SSH access from anywhere
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # All ports open within the VPC
  ingress {
    from_port   = 0
    to_port     = 65535
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }

  # outbound internet access
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "Benchmark-Security-Group-${random_id.hash.hex}"
  }
}


resource "aws_key_pair" "auth" {
  key_name   = "${var.key_name}-${random_id.hash.hex}"
  public_key = file(var.public_key_path)
}


resource "aws_instance" "bookie" {
  ami           = var.ami
  instance_type = var.instance_types["bookkeeper"]
  key_name      = aws_key_pair.auth.id
  subnet_id     = aws_subnet.benchmark_subnet.id
  vpc_security_group_ids = [ aws_security_group.benchmark_security_group.id ]
  count         = var.num_instances["bookkeeper"]

  tags = {
    Name = "bookie-${count.index}"
  }
}

resource "aws_instance" "zookeeper" {
  ami           = var.ami
  instance_type = var.instance_types["zookeeper"]
  key_name      = aws_key_pair.auth.id
  subnet_id     = aws_subnet.benchmark_subnet.id
  vpc_security_group_ids = [aws_security_group.benchmark_security_group.id]
  count         = var.num_instances["zookeeper"]

  tags = {
    Name = "zk-${count.index}"
  }
}

resource "aws_instance" "client" {
  ami           = var.ami
  instance_type = var.instance_types["client"]
  key_name      = aws_key_pair.auth.id
  subnet_id     = aws_subnet.benchmark_subnet.id
  vpc_security_group_ids = [aws_security_group.benchmark_security_group.id]
  count = var.num_instances["client"]

  tags = {
    Name = "bookkeeper-client-${count.index}"
  }
}


output "zookeeper" {
  value = {
    for instance in aws_instance.zookeeper :
    instance.public_ip => instance.private_ip
  }
}

output "bookie" {
  value = {
    for instance in aws_instance.bookie :
    instance.public_ip => instance.private_ip
  }
}

output "client" {
  value = {
    for instance in aws_instance.client :
    instance.public_ip => instance.private_ip
  }
}

resource "local_file" "ansible_inventory" {
  content = <<EOF
[bookies]
%{ for instance in aws_instance.bookie }
${instance.tags.Name} ansible_host=${instance.private_ip}
%{ endfor }

[zookeepers]
%{ for instance in aws_instance.zookeeper }
${instance.tags.Name} ansible_host=${instance.private_ip}
%{ endfor }

[clients]
%{ for instance in aws_instance.client }
${instance.tags.Name} ansible_host=${instance.private_ip}
%{ endfor }
EOF

  filename = "${path.module}/inventory.ini"
}

