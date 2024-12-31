terraform destroy -target=harvester_virtualmachine.workervm -target=harvester_virtualmachine.hostvm
terraform apply -target=harvester_virtualmachine.hostvm -target=harvester_virtualmachine.workervm --auto-approve
bash tools/clean.sh
ansible-playbook -i generate_inventory.py ansible/full.yaml