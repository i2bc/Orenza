#PBS -l walltime=48:00:00
#PBS -l mem=8Gb
#PBS -N update_orenza
#PBS -l ncpus=1
#PBS -q common
#PBS -j oe

mkdir -p /scratchlocal/chloe.quignot/update_orenza/
cp -r /data/work/I2BC/chloe.quignot/test_orenza/script /scratchlocal/chloe.quignot/update_orenza/
rm -r /data/work/I2BC/chloe.quignot/test_orenza/script/db
mkdir -p /data/work/I2BC/chloe.quignot/test_orenza/script/db
cp /data/work/I2BC/chloe.quignot/test_orenza/script/db_orenza.sqlite3 /data/work/I2BC/chloe.quignot/test_orenza/script/db
cd /scratchlocal/chloe.quignot/update_orenza/script
python3 update.py
mv *log db/* /data/work/I2BC/chloe.quignot/test_orenza/script/db/
cd -
#rm -r /scratchlocal/chloe.quignot/update_orenza/
