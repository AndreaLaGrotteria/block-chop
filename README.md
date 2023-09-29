# BlockChop: Chop-Chop library with support for Ethereum blockchain
Chop-Chop is an accelerator for Consensus primitivies developed by the Distributed Computing Laboratory at EPFL. This forked version of Chop-Chop adds support for EVM.
Note: BlockChop uses a forked version of the Zebra library.
## Usage
### Consensus Smart Contract
A basic version of the contract to be deployed is provided in the repo with the file `Consensus.sol`.

Please note that the current implementation uses a function named `submitTest` with an array of uint8 as payload and expects a `newSubmission` event that carries as well an array of uint8.

### Local node
The system can by started using the `launch_system.zsh` script. It takes the following arguments in order: number of servers, number of clients, 
number of load brokers, number of honest brokers, number of load clients, number of honest clients, flag whether to use the blockchain (true) or the loopback reference (false). 

The script need to be modified in the variable `addr` with the following info, comma separated: address to which the the Consensus contract has been deployed, address of the account that will be used be used by the node to make the transactions, IPC address of the local node.

After testing, the system can be terminated with the script `terminate_system.zsh`.
