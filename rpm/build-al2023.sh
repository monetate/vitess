### al2023 / el9

### Configure instance

# modify root volume 20 GB
# https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/recognize-expanded-volume-linux.html
sudo growpart /dev/nvme0n1 1
sudo xfs_growfs -d /

# disable selinux, needed for al2023?
sudo setenforce 0

### Install build dependencies

# git
sudo yum install -y git

# go 1.21
sudo yum install -y golang  # go 1.20.12
go install golang.org/dl/go1.22.4@latest
${HOME}/go/bin/go1.22.4 download
export PATH=${HOME}/sdk/go1.22.4/bin:${PATH}

# node 18
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.7/install.sh | bash
export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion
nvm install 20  # minimum 20.12.0, install 20.15.0
nvm use 20      # minimum 20.12.0, use     20.15.0

### Build vitess

git clone https://github.com/monetate/vitess
cd vitess
git checkout monetate-v20.0.0

make build

### Build etcd

sudo yum install -y maven
make tools
