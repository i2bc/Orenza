mkdir -p /scratchlocal/chloe.quignot/update_orenza/
cp -r /data/work/I2BC/chloe.quignot/test_orenza/script /scratchlocal/chloe.quignot/update_orenza/
cd /scratchlocal/chloe.quignot/update_orenza/script
python3 update.py
mv *log db/* /data/work/I2BC/chloe.quignot/test_orenza/script/db/
cd -
rm -r /scratchlocal/chloe.quignot/update_orenza/
