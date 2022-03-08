for k in 1 2 4 8 16; do
  echo "$k Real-data\n"
        # echo "2VTO"
        # for i in 1 2 3 4 5; do
        # ./setup.sh
        # nice -n -20 /sbin/mobibench_2VTO -j 5 -d 3 -T 1 -p /cache -t 4
        # done;
        echo "Baseline"
        for i in 1 2 3 4 5; do
        ./setup.sh
        nice -n -20 /sbin/mobibench_stock -j 3 -d 3 -T 1 -p /cache -t 4
        done;
        echo "Begin-Concurrent"
        for i in 1 2 3 4 5; do
        ./setup.sh
        nice -n -20 ./mobibench_BC -j 3 -d 3 -T 1 -p /cache -t 4
        done;
done;