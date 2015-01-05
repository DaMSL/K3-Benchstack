for q in queries/*; do
    echo $q:
    sudo LD_LIBRARY_PATH=../cli ../cli/sqlplus system/manager@localhost:12521/orcl <<< @$q | grep Elapsed
done
