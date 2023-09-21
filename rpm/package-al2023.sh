### al2023 / el9

### Build artifact

sudo yum -y install rpmdevtools

rpmdev-setuptree
tar -cvf $HOME/rpmbuild/SOURCES/monetate-vitess.tar vitess/bin vitess/dist/etcd/etcd-v3.5.6-linux-amd64 vitess/web/vtadmin/build
rpmbuild -bb vitess/rpm/monetate-vitess.spec

### Upload artifact

package=monetate-vitess
version=21.0.4
release=1.amzn2023

baseurl=https://monetate.jfrog.io/artifactory/base-amzn-2023-local/2023

if [ -z "$JFROG_API_KEY" ]; then
	JFROG_API_KEY=$(aws s3 cp s3://secret-monetate-dev/artifactory/monetate.jfrog.io/build-writer/api-key.txt -)
fi

curl --request PUT \
  --header "X-JFrog-Art-Api:${JFROG_API_KEY}" \
  --upload-file rpmbuild/RPMS/x86_64/${package}-${version}-${release}.x86_64.rpm \
  ${baseurl}/${package}-${version}-${release}.x86_64.rpm
