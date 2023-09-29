// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.9;

// Uncomment this line to use console.log
import "hardhat/console.sol";

contract Consensus{
    mapping(bytes32 => uint) public transactions;
    uint private count = 0;

    event newSubmission(uint8[] _payload);

    function submitTest(uint8[] calldata _payload) public payable{
        console.log("Received payload %d",_payload[0]);
        bytes32 hash = keccak256(abi.encodePacked(_payload));
        require(transactions[hash] == 0, "Transaction already submitted");
        count += 1;
        transactions[hash] = count;
        emit newSubmission(_payload);
    }

    function countTransactions() public view returns (uint){
        return count;
    }

}