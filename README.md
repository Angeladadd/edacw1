# Coursework for Engineering for Data Analysis 1

@author: Chenge Sun <chenge.sun.24@ucl.ac.uk>

### Deployment

#### Preparation
1. Install git, ansible and terraform

```sh
sudo dnf install -y epel-release
sudo dnf install -y git ansible

# https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli
sudo dnf install -y dnf-plugins-core
sudo dnf config-manager --add-repo https://rpm.releases.hashicorp.com/RHEL/hashicorp.repo
sudo dnf install -y terraform
```

2. disable host key checking(optional)
```sh
vim  ~/.ssh/config
# add the following conf to ~/.ssh/config
Host *
        StrictHostKeyChecking accept-new
```

#### Clone git repo
3. clone repo and submodules
   ```sh
   ##### clone repository with merizo submodule
   git clone git@github.com:Angeladadd/edacw1.git
   ##### init merizo search
   cd edacw1/
   git submodule update --init --recursive
   ```

#### Test existing cluster
Note: the following scripts are only used to test existing cluster
```
git checkout test_current_cluster
cd environment
bash ../tools/fetch_miniopass.sh
# this step will run an integration test on a subset of ecoli
ansible-playbook -i inventory.yaml ansible/test.yaml
```

#### Deploy new cluster

4. update `namespace`, `username`, `network_name`, and `keyname` in [environment/variables.tf](https://github.com/Angeladadd/edacw1/blob/main/environment/variables.tf)

   This step is necessary to ensure that developers can use their resources and access the created machines.
   After changing the ```username```, exposed endpoints listed below should be changed to \<username\>-\<hostname\>.comp0235.condenser.arc.ucl.ac.uk correspondingly, for example, \<username\>-grafana.comp0235.condenser.arc.ucl.ac.uk.

5. setup machines and environments
   ```sh
   ##### create vms
   cd environment
   terraform init
   terraform apply
   ##### install dependencies
   chmod +x generate_inventory.py
   # clean known hosts
   bash ../tools/clean.sh
   ansible-playbook -i generate_inventory.py ansible/site.yaml
   ```

#### Run datasets
6. run analysis pipeline for specific datasets

  For Human dataset

```sh
  ansible-playbook -i generate_inventory.py ansible/run_human_dataset.yaml
  ```

  For Ecoli dataset

```sh
  ansible-playbook -i generate_inventory.py ansible/run_ecoli_dataset.yaml
  ```

7. use different datasets(optional)

  two steps are required for using different datasets other than ecoli and human

  - update the data loading playbook: [environment/ansible/data.yaml](https://github.com/Angeladadd/edacw1/blob/main/environment/ansible/data.yaml#L12). configure to download the new dataset and create input and output buckets
  - create a analysis playbook as of the existing datasets: [environment/ansible/run_human_dataset.yaml](https://github.com/Angeladadd/edacw1/blob/main/environment/ansible/run_human_dataset.yaml).
  configure the necessary parameters to run the analysis script. (Hint: adjust partitions for different size of input to get better performance. a recommendation is keep a single partition less than 100 rows)

8. run validation test to validate the pipeline result(optional)
  ```sh
  ansible-playbook -i generate_inventory.py ansible/validation.yaml
  ```


### Access Results

Researchers can access result from Minio UI or command line tools

1. open bucket url

https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/browser/ecoli-cath-parsed

https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/browser/human-cath-parsed

https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/browser/cath-summary

2. get via curl

```sh
curl -O https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/human-cath-parsed/AF-A0A024RBG1-F1-model_v4.parsed
```
  If .parsed file not found, download and check \_segement.tsv for help

```sh
curl -O https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/human-cath-parsed/AF-A0A024RBG1-F1-model_v4_segment.tsv
```

### Monitoring and Management

- [Grafana Dashboard](https://ucabc46-grafana.comp0235.condenser.arc.ucl.ac.uk/d/yarn-cluster-resource-hostnode/yarn-cluster-resource?orgId=1): Username: admin, Password: admin
- [Yarn Resource Manager](https://ucabc46-yarn.comp0235.condenser.arc.ucl.ac.uk/cluster)
- Yarn Node Manager
  - [Workernode1](https://ucabc46-workernode1.comp0235.condenser.arc.ucl.ac.uk/logs/)
  - [Workerndoe2](https://ucabc46-workernode2.comp0235.condenser.arc.ucl.ac.uk/logs/)
  - [Workerndoe3](https://ucabc46-workernode3.comp0235.condenser.arc.ucl.ac.uk/logs/)
- [Spark History Server](https://ucabc46-sparkhistory.comp0235.condenser.arc.ucl.ac.uk/)
- [MinIO Web UI](https://ucabc46-cons.comp0235.condenser.arc.ucl.ac.uk/login): User: myminioadmin, Password:
  - 1. Check cnc machine to get password: ```cat <path to>/edacw1/environment/ansible/.miniopass```
  - 2. Or check storage node to get password: ```cat /home/almalinux/miniopass```


### Development

1. sync with remote machine

install rsync on remote machine and create remote target folder

```sh
sudo dnf install rsync
cd ~
git clone git@github.com:Angeladadd/edacw1.git
cd ~/edacw1
git submodule update --init --recursive
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
