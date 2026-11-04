// SPDX-License-Identifier: MIT
pragma solidity 0.8.24;

/// @title BalanceScanner (deployless)
/// @notice Never deployed. Sent as the `data` of an eth_call with no `to`, so the node runs
///         the constructor and returns whatever the constructor returns.
/// @dev    Input : tokens  = asset list, address(0) means the chain's native coin
///                 packed  = wallets, 20 raw bytes each, back to back
///         Output: only non-zero (or failed) entries, 37 bytes each:
///                 uint32 walletIndex | uint8 assetIndex (bit 7 set = call failed) | uint256 balance
///         Most scanned wallets are empty, so responses stay tiny.
contract BalanceScanner {
    constructor(address[] memory tokens, bytes memory packed) payable {
        assembly {
            let n := div(mload(packed), 20)
            let nt := mload(tokens)
            let src := add(packed, 32)
            let tok0 := add(tokens, 32)
            let out := mload(0x40)
            let w := out
            for { let i := 0 } lt(i, n) { i := add(i, 1) } {
                let a := shr(96, mload(add(src, mul(i, 20))))
                for { let t := 0 } lt(t, nt) { t := add(t, 1) } {
                    let tok := mload(add(tok0, mul(t, 32)))
                    let bal := 0
                    let ok := 1
                    switch tok
                    case 0 { bal := balance(a) }
                    default {
                        mstore(0x00, shl(224, 0x70a08231))
                        mstore(0x04, a)
                        ok := staticcall(gas(), tok, 0x00, 0x24, 0x00, 0x20)
                        if lt(returndatasize(), 32) { ok := 0 }
                        switch ok
                        case 0 { bal := 0 }
                        default { bal := mload(0x00) }
                    }
                    if or(bal, iszero(ok)) {
                        mstore(w, or(shl(224, i), shl(216, or(t, mul(iszero(ok), 0x80)))))
                        mstore(add(w, 5), bal)
                        w := add(w, 37)
                    }
                }
            }
            return(out, sub(w, out))
        }
    }
}
