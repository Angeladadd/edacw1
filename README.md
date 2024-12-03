# Coursework for Engineering for Data Analysis 1

### Deployment

```sh
cd <path to repo>/environment
# create vms
terraform init
terraform apply
# install dependencies
chmod +x generate_inventory.py
ansible-playbook -i generate_inventory.py ansible/full.yaml
```

### Verification

1. verify the data storage node

```sh
[almalinux@ucabc46-storage-01-36292bae40 ~]$ lsblk
NAME   MAJ:MIN RM  SIZE RO TYPE MOUNTPOINTS
vda    252:0    0   10G  0 disk
├─vda1 252:1    0    1M  0 part
├─vda2 252:2    0  200M  0 part /boot/efi
├─vda3 252:3    0    1G  0 part /boot
└─vda4 252:4    0  8.8G  0 part /
vdb    252:16   0  200G  0 disk
└─vdb1 252:17   0  200G  0 part /data
vdc    252:32   0    1M  0 disk
[almalinux@ucabc46-storage-01-36292bae40 ~]$ df -h
Filesystem      Size  Used Avail Use% Mounted on
devtmpfs        4.0M     0  4.0M   0% /dev
tmpfs           3.9G     0  3.9G   0% /dev/shm
tmpfs           1.6G  8.7M  1.6G   1% /run
/dev/vda4       8.8G 1011M  7.8G  12% /
/dev/vda3       960M  130M  831M  14% /boot
/dev/vda2       200M  7.1M  193M   4% /boot/efi
/dev/vdb1       200G  1.5G  199G   1% /data
tmpfs           784M     0  784M   0% /run/user/1000
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
