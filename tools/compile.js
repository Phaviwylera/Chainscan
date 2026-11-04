const solc = require("solc");
const fs = require("fs");
const path = require("path");
const dir = process.argv[2];
const sources = {};
for (const f of fs.readdirSync(dir).filter(f => f.endsWith(".sol")))
  sources[f] = { content: fs.readFileSync(path.join(dir, f), "utf8") };
const input = { language: "Solidity", sources,
  settings: { optimizer: { enabled: true, runs: 200 }, evmVersion: "paris",
    outputSelection: { "*": { "*": ["evm.bytecode.object", "abi"] } } } };
const out = JSON.parse(solc.compile(JSON.stringify(input)));
if (out.errors) for (const e of out.errors) console.error(e.formattedMessage);
const res = {};
for (const f in out.contracts) for (const c in out.contracts[f])
  res[c] = { bytecode: "0x" + out.contracts[f][c].evm.bytecode.object, abi: out.contracts[f][c].abi };
fs.writeFileSync(process.argv[3], JSON.stringify(res, null, 1));
console.log("compiled:", Object.keys(res).map(k => `${k} ${(res[k].bytecode.length - 2) / 2}B`).join(", "));
