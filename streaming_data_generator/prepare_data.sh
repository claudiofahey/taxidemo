set -ex
infile=~/Downloads/yellow_tripdata_2015-05.csv
outfile=sorted_tripdata.csv
ls -lh ${infile}
head -1 ${infile} > ${outfile}
cat ${infile} | tail -n +2 | sort -t , -k 3 --buffer-size 4G >> ${outfile}
ls -lh ${outfile}
