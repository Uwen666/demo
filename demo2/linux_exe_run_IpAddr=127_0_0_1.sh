#!/bin/bash
# Stop previous BlockEmulator processes to avoid port conflicts and ghost runs.
bash ./stop_blockemulator.sh >/dev/null 2>&1 || true

./blockEmulator -n 0 -N 4 -s 0 -S 20 & 

./blockEmulator -n 1 -N 4 -s 0 -S 20 & 

./blockEmulator -n 2 -N 4 -s 0 -S 20 & 

./blockEmulator -n 3 -N 4 -s 0 -S 20 & 

./blockEmulator -n 0 -N 4 -s 1 -S 20 & 

./blockEmulator -n 1 -N 4 -s 1 -S 20 & 

./blockEmulator -n 2 -N 4 -s 1 -S 20 & 

./blockEmulator -n 3 -N 4 -s 1 -S 20 & 

./blockEmulator -n 0 -N 4 -s 2 -S 20 & 

./blockEmulator -n 1 -N 4 -s 2 -S 20 & 

./blockEmulator -n 2 -N 4 -s 2 -S 20 & 

./blockEmulator -n 3 -N 4 -s 2 -S 20 & 

./blockEmulator -n 0 -N 4 -s 3 -S 20 & 

./blockEmulator -n 1 -N 4 -s 3 -S 20 & 

./blockEmulator -n 2 -N 4 -s 3 -S 20 & 

./blockEmulator -n 3 -N 4 -s 3 -S 20 & 

./blockEmulator -n 0 -N 4 -s 4 -S 20 & 

./blockEmulator -n 1 -N 4 -s 4 -S 20 & 

./blockEmulator -n 2 -N 4 -s 4 -S 20 & 

./blockEmulator -n 3 -N 4 -s 4 -S 20 & 

./blockEmulator -n 0 -N 4 -s 5 -S 20 & 

./blockEmulator -n 1 -N 4 -s 5 -S 20 & 

./blockEmulator -n 2 -N 4 -s 5 -S 20 & 

./blockEmulator -n 3 -N 4 -s 5 -S 20 & 

./blockEmulator -n 0 -N 4 -s 6 -S 20 & 

./blockEmulator -n 1 -N 4 -s 6 -S 20 & 

./blockEmulator -n 2 -N 4 -s 6 -S 20 & 

./blockEmulator -n 3 -N 4 -s 6 -S 20 & 

./blockEmulator -n 0 -N 4 -s 7 -S 20 & 

./blockEmulator -n 1 -N 4 -s 7 -S 20 & 

./blockEmulator -n 2 -N 4 -s 7 -S 20 & 

./blockEmulator -n 3 -N 4 -s 7 -S 20 & 

./blockEmulator -n 0 -N 4 -s 8 -S 20 & 

./blockEmulator -n 1 -N 4 -s 8 -S 20 & 

./blockEmulator -n 2 -N 4 -s 8 -S 20 & 

./blockEmulator -n 3 -N 4 -s 8 -S 20 & 

./blockEmulator -n 0 -N 4 -s 9 -S 20 & 

./blockEmulator -n 1 -N 4 -s 9 -S 20 & 

./blockEmulator -n 2 -N 4 -s 9 -S 20 & 

./blockEmulator -n 3 -N 4 -s 9 -S 20 & 

./blockEmulator -n 0 -N 4 -s 10 -S 20 & 

./blockEmulator -n 1 -N 4 -s 10 -S 20 & 

./blockEmulator -n 2 -N 4 -s 10 -S 20 & 

./blockEmulator -n 3 -N 4 -s 10 -S 20 & 

./blockEmulator -n 0 -N 4 -s 11 -S 20 & 

./blockEmulator -n 1 -N 4 -s 11 -S 20 & 

./blockEmulator -n 2 -N 4 -s 11 -S 20 & 

./blockEmulator -n 3 -N 4 -s 11 -S 20 & 

./blockEmulator -n 0 -N 4 -s 12 -S 20 & 

./blockEmulator -n 1 -N 4 -s 12 -S 20 & 

./blockEmulator -n 2 -N 4 -s 12 -S 20 & 

./blockEmulator -n 3 -N 4 -s 12 -S 20 & 

./blockEmulator -n 0 -N 4 -s 13 -S 20 & 

./blockEmulator -n 1 -N 4 -s 13 -S 20 & 

./blockEmulator -n 2 -N 4 -s 13 -S 20 & 

./blockEmulator -n 3 -N 4 -s 13 -S 20 & 

./blockEmulator -n 0 -N 4 -s 14 -S 20 & 

./blockEmulator -n 1 -N 4 -s 14 -S 20 & 

./blockEmulator -n 2 -N 4 -s 14 -S 20 & 

./blockEmulator -n 3 -N 4 -s 14 -S 20 & 

./blockEmulator -n 0 -N 4 -s 15 -S 20 & 

./blockEmulator -n 1 -N 4 -s 15 -S 20 & 

./blockEmulator -n 2 -N 4 -s 15 -S 20 & 

./blockEmulator -n 3 -N 4 -s 15 -S 20 & 

./blockEmulator -n 0 -N 4 -s 16 -S 20 & 

./blockEmulator -n 1 -N 4 -s 16 -S 20 & 

./blockEmulator -n 2 -N 4 -s 16 -S 20 & 

./blockEmulator -n 3 -N 4 -s 16 -S 20 & 

./blockEmulator -n 0 -N 4 -s 17 -S 20 & 

./blockEmulator -n 1 -N 4 -s 17 -S 20 & 

./blockEmulator -n 2 -N 4 -s 17 -S 20 & 

./blockEmulator -n 3 -N 4 -s 17 -S 20 & 

./blockEmulator -n 0 -N 4 -s 18 -S 20 & 

./blockEmulator -n 1 -N 4 -s 18 -S 20 & 

./blockEmulator -n 2 -N 4 -s 18 -S 20 & 

./blockEmulator -n 3 -N 4 -s 18 -S 20 & 

./blockEmulator -n 0 -N 4 -s 19 -S 20 & 

./blockEmulator -n 1 -N 4 -s 19 -S 20 & 

./blockEmulator -n 2 -N 4 -s 19 -S 20 & 

./blockEmulator -n 3 -N 4 -s 19 -S 20 & 

./blockEmulator --standby --standbyID 0 --bindAddr 127.0.0.1:18889 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 1 --bindAddr 127.0.0.1:18890 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 2 --bindAddr 127.0.0.1:18891 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 3 --bindAddr 127.0.0.1:18892 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 4 --bindAddr 127.0.0.1:18893 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 5 --bindAddr 127.0.0.1:18894 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 6 --bindAddr 127.0.0.1:18895 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 7 --bindAddr 127.0.0.1:18896 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 8 --bindAddr 127.0.0.1:18897 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 9 --bindAddr 127.0.0.1:18898 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 10 --bindAddr 127.0.0.1:18899 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 11 --bindAddr 127.0.0.1:18900 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 12 --bindAddr 127.0.0.1:18901 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 13 --bindAddr 127.0.0.1:18902 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 14 --bindAddr 127.0.0.1:18903 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 15 --bindAddr 127.0.0.1:18904 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 16 --bindAddr 127.0.0.1:18905 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 17 --bindAddr 127.0.0.1:18906 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 18 --bindAddr 127.0.0.1:18907 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 19 --bindAddr 127.0.0.1:18908 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 20 --bindAddr 127.0.0.1:18909 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 21 --bindAddr 127.0.0.1:18910 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 22 --bindAddr 127.0.0.1:18911 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 23 --bindAddr 127.0.0.1:18912 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 24 --bindAddr 127.0.0.1:18913 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 25 --bindAddr 127.0.0.1:18914 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 26 --bindAddr 127.0.0.1:18915 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 27 --bindAddr 127.0.0.1:18916 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 28 --bindAddr 127.0.0.1:18917 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 29 --bindAddr 127.0.0.1:18918 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 30 --bindAddr 127.0.0.1:18919 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 31 --bindAddr 127.0.0.1:18920 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 32 --bindAddr 127.0.0.1:18921 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 33 --bindAddr 127.0.0.1:18922 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 34 --bindAddr 127.0.0.1:18923 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 35 --bindAddr 127.0.0.1:18924 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 36 --bindAddr 127.0.0.1:18925 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 37 --bindAddr 127.0.0.1:18926 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 38 --bindAddr 127.0.0.1:18927 -N 4 -S 20 & 

./blockEmulator --standby --standbyID 39 --bindAddr 127.0.0.1:18928 -N 4 -S 20 & 

./blockEmulator -c -N 4 -S 20 & 

