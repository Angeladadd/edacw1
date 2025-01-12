# Coursework for Engineering for Data Analysis 1

@author: Chenge Sun <chenge.sun.24@ucl.ac.uk>

### Deployment

1. Install ansible

```sh
sudo dnf install python3-pip
python3 -m pip install --user ansible
```

2. disable host key checking
```sh
vim  ~/.ssh/config
# add the following conf to ~/.ssh/config
Host *
        StrictHostKeyChecking accept-new
```
3. setup machines and environments
```sh
##### clone repository
cd ~
git clone git@github.com:Angeladadd/edacw1.git
cd ~/edacw1/environment
##### create vms
terraform init
terraform apply
##### install dependencies
chmod +x generate_inventory.py
# optional: clean known hosts
# bash ../tools/clean.sh
ansible-playbook -i generate_inventory.py ansible/site.yaml
```

4. run analysis pipeline for specific datasets

  For Human dataset

```sh
  ansible-playbook -i generate_inventory.py ansible/run_human_dataset.yaml
  ```

  For Ecoli dataset

```sh
  ansible-playbook -i generate_inventory.py ansible/run_ecoli_dataset.yaml
  ```

5. use different datasets

  two steps are required for using different datasets other than ecoli and human

  - update the data loading playbook: [environment/ansible/data.yaml](https://github.com/Angeladadd/edacw1/blob/main/environment/ansible/data.yaml#L12)。 configure to download the new dataset and create input and output buckets
  - create a analysis playbook as of the existing datasets: [environment/ansible/run_human_dataset.yaml](https://github.com/Angeladadd/edacw1/blob/main/environment/ansible/run_human_dataset.yaml).
  configure the necessary parameters to run the analysis script. (Hint: adjust partitions for different size of input to get better performance. a recommendation is keep a single partition less than 100 rows)

6. run validation test to validate the pipeline result(optional)
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

### Monitoring

Grafana Dashboard: https://ucabc46-grafana.comp0235.condenser.arc.ucl.ac.uk/d/yarn-cluster-resource-hostnode/yarn-cluster-resource?orgId=1

Username: admin, Password: admin

### Development

1. sync with remote machine

install rsync on remote machine and create remote target folder

```sh
sudo dnf install rsync
cd ~
git clone git@github.com:Angeladadd/edacw1.git
cd ~/edacw1
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
