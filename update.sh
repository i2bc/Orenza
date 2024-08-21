#PBS -l walltime=00:10:00
#PBS -l mem=100Mb
#PBS -N update_orenza
#PBS -l ncpus=1
#PBS -q common
#PBS -j oe

CONFIG=config.yaml
function query(){
    python -c 'import yaml,sys;config=yaml.safe_load(sys.stdin);print(config["download"].get("'$1'",""))' < $CONFIG
}

CMD="/usr/bin/python3 update.py"
QSUB_OPTIONS="-l walltime=48:00:00 -l ncpus=1 -q common -j oe -N update_orenza -l mem=500Mb"
QSUB_OPTIONS_PDB="-l walltime=48:00:00 -l ncpus=20 -q common -j oe -N update_orenza -l mem=500Mb"
QSUB_OPTIONS_POP="-l walltime=48:00:00 -l ncpus=1 -q common -j oe -N update_orenza -l mem=8Gb"
JOBIDS=()

RES=$(query "explorenz")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS -- $CMD "dl_explorenz"))
fi
RES=$(query "explorenz")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS -- $CMD "dl_explorenz"))
fi
RES=$(query "sprot")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS -- $CMD "dl_sprot"))
fi
RES=$(query "trembl")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS -- $CMD "dl_trembl"))
fi
RES=$(query "kegg")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS -- $CMD "dl_kegg"))
fi
RES=$(query "brenda")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS -- $CMD "dl_brenda"))
fi
RES=$(query "pdb")
if [[ $RES == "True" || $RES == "true" ]]; then 
    JOBIDS+=($(qsub $QSUB_OPTIONS_PDB -- $CMD "dl_pdb"))
fi

if [[ ${#JOBIDS[@]} -gt 0 ]]; then
    qsub $QSUB_OPTIONS_POP -W depend=afterok:$(IFS=: ; echo "${JOBIDS[*]}") -- $CMD "populate"
fi
