### al2023 / el9

### install test dependencies

# mysql
sudo yum install -y https://dev.mysql.com/get/mysql80-community-release-el9-5.noarch.rpm
sudo yum install -y mysql-community-server-8.0.35
sudo systemctl stop mysqld
sudo systemctl disable mysqld

# xtrabackup
sudo yum install -y https://downloads.percona.com/downloads/Percona-XtraBackup-8.0/Percona-XtraBackup-8.0.35-30/binary/redhat/9/x86_64/percona-xtrabackup-80-8.0.35-30.1.el9.x86_64.rpm

sudo yum install -y ant maven zip gcc

### Test vitess

make tools
make unit_test