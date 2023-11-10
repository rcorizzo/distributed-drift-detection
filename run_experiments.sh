for MULT_DATA in  64 128 256 512
do
    for INSTANCES in 16 8 4 2 1
    do
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 2gb 2 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 4gb 2 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 8gb 2 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 2gb 4 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 4gb 4 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 8gb 4 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 2gb 8 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 4gb 8 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
        python DDM_process.py spark://***.***.***.***:7077 $INSTANCES 8gb 8 $(date | sed 's/ //g' | sed 's/://g') $MULT_DATA
    done
done