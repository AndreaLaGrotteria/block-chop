# BlockChop: Chop-Chop library with support for Ethereum blockchain
Chop-Chop is an accelerator for Consensus primitivies developed by the Distributed Computing Laboratory at EPFL. This forked version of Chop-Chop adds support for EVM.
Note: BlockChop uses a forked version of the Zebra library.
## Usage
The system can by started using the `launch_system.zsh` script. It takes the following arguments in order: number of servers, number of clients, 
number of load brokers, number of honest brokers, number of load clients, number of honest clients, flag whether to use the blockchain (true) or the loopback reference (false). 

The script need to be modified in the variables `addr` and `addr2` with the following info, comma separated: address to which the the Consensus contract has been deployed, 
address of the account that will be used be used by the node to make the transactions, IPC address of the local node.
