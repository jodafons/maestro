squeue -u $USER -h | awk '{print $1}' | xargs scancel
rm *.out