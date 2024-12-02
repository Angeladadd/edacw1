# sudo dnf install rsync
# vim ~/.ssh/config

find /Users/sunchenge/Desktop/UCL/EDA1/cw/edacw1 | entr -r rsync -avz --progress /Users/sunchenge/Desktop/UCL/EDA1/cw/edacw1 condenser-vm1:/home/almalinux/