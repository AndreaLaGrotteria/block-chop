#!/bin/zsh

num_servers=$1
num_clients=$2
num_load_brokers=$3
num_honest_brokers=$4
num_load_clients=$5
num_honest_clients=$6
use_blockchain=$7

addr='0xc17Ae51dD5d90c11dB677b5d3Fb387a887073BA8;0x66f4B9340e31ef251EF82f7842827d947b16dAc4;/Users/andrealagrotteria/EPFL/SemesterProject/clean-try/node4/geth.ipc'
# addr2='0xc17Ae51dD5d90c11dB677b5d3Fb387a887073BA8;0x66f4B9340e31ef251EF82f7842827d947b16dAc4;/Users/andrealagrotteria/EPFL/SemesterProject/clean-try/node3/geth.ipc'
./target/release/generate_system $num_servers $num_clients ./passepartout.db ./membership.db ./directory.db

x=$(($num_clients*5/10))
y=$num_clients
z=$((($x+$y)/2))
k=$num_load_brokers

./target/release/generate_batches ./passepartout.db ./directory.db $x $y 16 $k 320 6 1 ./flows/ --random

./target/release/rendezvous 1234 $((num_servers+num_load_brokers+num_honest_brokers+num_load_clients+num_honest_clients)) &
sleep 1

if [ "$use_blockchain" = true ] ; then
    i=0
    while [ $i -lt $num_servers ]
    do
        ./target/release/server 127.0.0.1:1234 ./membership.db  $num_servers $i ./passepartout.db ./directory.db --blockchain $addr --random --heartbeat-path ./heartbeat_server.bin &
        i=$(( $i + 1 ))
    done
    # ./target/release/server 127.0.0.1:1234 ./membership.db  $num_servers 0 ./passepartout.db ./directory.db --blockchain $addr --random --heartbeat-path ./heartbeat_server.bin &
    # ./target/release/server 127.0.0.1:1234 ./membership.db  $num_servers 1 ./passepartout.db ./directory.db --blockchain $addr2 --random --heartbeat-path ./heartbeat_server.bin &

else
    i=0
    while [ $i -lt $num_servers ]
    do
        ./target/release/server 127.0.0.1:1234 ./membership.db  $num_servers $i ./passepartout.db ./directory.db --loopback --random --heartbeat-path ./heartbeat_server.bin &
        i=$(( $i + 1 ))
    done
fi

i=0
while [ $i -lt $num_load_brokers ]
do
    ./target/release/load_broker 127.0.0.1:1234 ./membership.db $num_servers 1 ./flows/ $i $(($i+1)) --batches-per-flow 320 --heartbeat-path ./heartbeat_L_broker.bin &
    i=$(( $i + 1 ))
done

i=0
while [ $i -lt $num_honest_brokers ]
do
    ./target/release/honest_broker 127.0.0.1:1234 9500 ./membership.db $num_servers ./directory.db --heartbeat-path ./heartbeat_H_broker.bin &
    i=$(( $i + 1 ))
done

i=0
while [ $i -lt $num_load_clients ]
do
    ./target/release/load_client 127.0.0.1:1234 $((i+0)) 127.0.0.1:9500 50 60 ./passepartout.db ./directory.db --random &
    i=$(( $i + 1 ))
done

i=0
while [ $i -lt $num_honest_clients ]
do
    export RUST_LOG='off, honest_client=info, chop_chop=warn'
    ./target/release/honest_client 127.0.0.1:1234 $((i+$num_load_clients)) 127.0.0.1:9500 60 ./membership.db $num_servers ./passepartout.db ./directory.db --random &
    i=$(( $i + 1 ))
done

echo "System generated!"

wait