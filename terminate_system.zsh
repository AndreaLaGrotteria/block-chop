#!/bin/zsh

# Terminate all child processes spawned by the main script

# Terminate server processes
pids=$(pgrep -f server)
echo "Terminating server processes"
for p in ${(f)pids}
do
    # echo $p
    kill $p
done

# Terminate load broker processes
pids=$(pgrep -f load_broker)
echo "Terminating load broker processes"
for p in ${(f)pids}
do
    # echo $p
    kill $p
done

# Terminate honest broker processes
pids=$(pgrep -f honest_broker)
echo "Terminating honest broker processes"
for p in ${(f)pids}
do
    # echo $p
    kill $p
done

# Terminate load client processes
pids=$(pgrep -f load_client)
echo "Terminating load client processes"
for p in ${(f)pids}
do
    # echo $p
    kill $p
done

# Terminate honest client processes
pids=$(pgrep -f honest_client)
echo "Terminating honest client processes"
for p in ${(f)pids}
do
    # echo $p
    kill $p
done

# Terminate rendezvous process
pids=$(pgrep -f rendezvous)
if [ -n "$pids" ]; then
  echo "Terminating rendezvous process"
  kill $pids
fi
