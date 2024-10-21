cd ~
deactivate
git clone https://github.com/namoray/nineteen.git
cd nineteen
sudo -E ./bootstrap.sh
source $HOME/.bashrc
pm2 delete all
cat ~/vision/.default.env  # Or however you want to view your old config
python core/create_config.py