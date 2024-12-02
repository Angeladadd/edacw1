# Coursework for Engineering for Data Analysis 1

### Deployment

1. terraform

```
cd <path to repo>/terraform
terraform init
terraform apply
```


### Development

1. sync with remote machine

install rsync on remote machine and create remote target folder

```
sudo dnf install rsync
git clone
```

setup auto sync on local machine

```
vim ~/.ssh/config
# append in ~/.ssh/config
Host condenser-vm1
  HostName 10.134.12.8
  User almalinux
  ProxyJump condenser-proxy

sh sync.sh
```
