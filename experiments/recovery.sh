for k in 1 2 4 8 16; do
  echo "$k operation/txn\n"
        # echo "DHL"
        # for i in 1 2 3 4 5; do
        # rm -f /cache/test.db*
        # ./recovery_setup.sh
        # ./recovery_DHL -n 1500 -o $k -R 128 -j 0 -r 0
        # ./recovery_setup.sh
        # ./recovery_DHL -n 1500 -o $k -R 128 -j 0 -r 1
        # done;
        # echo "DASH"
        # for i in 1 2 3 4 5; do
        # rm -f /cache/test.db*
        # ./recovery_setup.sh
        # ./recovery_DASH -n 1500 -o $k -R 128 -j 5 -r 0
        # ./recovery_setup.sh
        # ./recovery_DASH -n 1500 -o $k -R 128 -j 5 -r 1
        # done;
        echo "WAL"
        for i in 1 2 3 4 5; do
        rm -f /cache/test.db*
        ./recovery_setup.sh
        ./recovery_vanilla -n 5000 -o $k -R 128 -j 1 -r 0
        ./recovery_setup.sh
        ./recovery_vanilla -n 5000 -o $k -R 128 -j 1 -r 1
        done;
        echo "IPS"
        for i in 1 2 3 4 5; do
        rm -f /cache/test.db*
        ./recovery_setup.sh
        ./recovery_IPS -n 5000 -o $k -R 128 -j 0 -r 0
        ./recovery_setup.sh
        ./recovery_IPS -n 5000 -o $k -R 128 -j 0 -r 1
        done;
done;