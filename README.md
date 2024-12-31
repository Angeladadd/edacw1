# Coursework for Engineering for Data Analysis 1

@author: Chenge Sun <chenge.sun.24@ucl.ac.uk>

### Deployment

1. disable host key checking
```sh
vim  ~/.ssh/config
# add the following conf to ~/.ssh/config
Host *
        StrictHostKeyChecking accept-new
```
2. setup machines and environments
```sh
# clone repository
git clone git@github.com:Angeladadd/edacw1.git
cd /path/to/edacw1/environment
# create vms
terraform init
terraform apply
# install dependencies
chmod +x generate_inventory.py
ansible-playbook -i generate_inventory.py ansible/full.yaml
# load dataset
ansible-playbook -i generate_inventory.py ansible/load_human_model.yaml
```

3. run the analysis script

    There are two ways to analysis pipeline:
    
    1. From the laptop/cnc-machine that provisions the VMs

    ```sh
    ansible-playbook -i generate_inventory.py ansible/run.yaml
    ```

    2. From host node we created in previous step

    ```sh
    ssh <hostnode ip>
    cd ~/pipeline
    
    ```


### Access Results

1. install minio client

  for Mac OS
  ```sh
  brew install minio/stable/mc
  ```

  for Linux
  ```sh
  wget https://dl.min.io/client/mc/release/linux-amd64/mc
  chmod +x mc
  sudo mv mc /usr/local/bin/
  ```

2. download .parsed file

```sh
curl -O https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/human-cath-parsed/AF-A0A024RBG1-F1-model_v4.parsed
```

### Monitoring

Grafana Dashboard: https://ucabc46-grafana.comp0235.condenser.arc.ucl.ac.uk/d/yarn-cluster-resource-hostnode/yarn-cluster-resource?orgId=1

Username: admin, Password: admin

### Development

1. sync with remote machine

install rsync on remote machine and create remote target folder

```sh
sudo dnf install rsync
git clone git@github.com:Angeladadd/edacw1.git
cd path/to/edacw1
```

setup auto sync on local machine

```sh
vim ~/.ssh/config
# append in ~/.ssh/config
Host condenser-vm1
  HostName 10.134.12.8
  User almalinux
  ProxyJump condenser-proxy

sh tools/sync.sh
```
