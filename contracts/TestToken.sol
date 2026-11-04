// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// Minimal ERC-20 used only by the local test suite.
contract TestToken {
    mapping(address => uint256) public balanceOf;
    function mintBatch(address[] calldata to, uint256[] calldata amt) external {
        for (uint256 i = 0; i < to.length; i++) balanceOf[to[i]] += amt[i];
    }
}
