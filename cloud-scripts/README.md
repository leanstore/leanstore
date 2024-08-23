# testdeploy
DM lab aws test deployment

# intall 
    terrafrom 
    aws cli - sudo apt-get install awscli
    ansible
# Runing the Terraform
    terraform init
    terraform plan
    terraform apply
    terraform output
# AWS cli
    aws configure

    export AWS_ACCESS_KEY_ID=your_access_key_id
    export AWS_SECRET_ACCESS_KEY=your_secret_access_key
    export AWS_DEFAULT_REGION=your_region


    aws --endpoint-url=http://localhost:4566 ec2 describe-instances --query 'Reservations[].Instances[].[InstanceId,InstanceType,PublicIpAddress,Tags[?Key==`Name`]| [0].Value]' --output table --region us-west-2
    ------------------------------------------------------------------------------
    |                              DescribeInstances                             |
    +----------------------+-----------+-----------------+-----------------------+
    |  i-336fe92b8bc02b11b |  t2.micro |  54.214.17.173  |  bookie-0             |
    |  i-fcc293f05e4ce3cc2 |  t2.micro |  54.214.54.195  |  zk-2                 |
    |  i-40aea0772e9447714 |  t2.micro |  54.214.92.201  |  bookkeeper-client-1  |
    |  i-206fd71491b2cdf18 |  t2.micro |  54.214.85.38   |  bookkeeper-client-0  |
    |  i-87a4fbedcb1076cb7 |  t2.micro |  54.214.70.224  |  zk-0                 |
    |  i-2509f9fe79c67902c |  t2.micro |  54.214.55.127  |  bookie-2             |
    |  i-3d88722677cbd9109 |  t2.micro |  54.214.35.9    |  zk-1                 |
    |  i-21b8593ea3847f47d |  t2.micro |  54.214.254.185 |  bookie-1             |
    +----------------------+-----------+-----------------+-----------------------+

# ansible
    ansible-playbook -i inventory-mock.ini testansibel.yml

# [Running Replicated ZooKeeper](https://zookeeper.apache.org/doc/current/zookeeperStarted.html#sc_RunningReplicatedZooKeeper)


# BK and Zk
/opt/bookkeeper/bin/bookkeeper shell listbookies -rw
/opt/zookeeper/bin/zkServer.sh status


 