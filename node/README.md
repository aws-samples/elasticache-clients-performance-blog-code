===Reproducing Test Results===

Prerequisites:
    To reproduce the results you must have an aws account and
    some basic familiarity with with the aws console is recommended.

Steps to Reproduce Result:
1) From the aws console, Launch a c5.4xlarge Amazon-Linux-2 EC2 instance in an availbility zone of your choice.
    For more info: https://ec2-immersionday.workshop.aws/launch-your-first-amazon-ec2-instance.html
2) From the aws console create an Elasticache instance in the same availability zone you used in step 1 using the specifications listed in the blog post.
    * Elasticache Specs:
        * cluster mode *disabled*
        * TLS *disabled*
        * 1 shard, 3 nodes, (1 primary, 2 replicas)
        * node types: r6g.2xlarge
    For more info: https://docs.aws.amazon.com/AmazonElastiCache/latest/red-ug/GettingStarted.html
3) SSH to the EC2 instance created in step 1:
    For more info: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html
4) Clone github respository to the EC2 instance created in step 1 using git clone.
5) Setup Node.js on the EC2 Instance from step 1 by running:
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
    . ~/.nvm/nvm.sh
    nvm install node
6) Install node dependencies by running: npm install
7) In blog.js replace "localhost" with "<Primary Endpoint>"
    where <Primary Endpoint> is the Primary Endpoint for the Elasticache instance you created in step 1
8) From the command line: node <Path to blog.js>

Cleaning Up:
Please make you sure you terminate the EC2 instance and delete the Elasticache cluster you created.