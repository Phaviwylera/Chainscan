"""AnalystAgent: Claude on Amazon Bedrock reads the finished run and writes report.md.

It never touches the hot path. Balance lookups are deterministic code; an LLM call per address
would be thousands of times slower and cost a fortune at 3 crore addresses. Claude is used where
judgment helps: explaining failures and endpoint behaviour, suggesting settings for the next run,
and summarising where the money sits.
"""
import csv
import json
import os

from .util import log

PROMPT = """You are the analyst agent for a bulk blockchain balance scan. Write a concise run report
in Markdown for the engineer who ran it. Sections:
1. Outcome: addresses scanned per chain, funded counts, totals per asset, duration and throughput.
2. Reliability: failed lookups, batch splits, block re-pins, endpoint errors / rate limits / parked
   endpoints. Say plainly whether the numbers can be trusted and what is missing.
3. Tuning for the next run: concrete changes (addresses per call, max_concurrency per endpoint,
   adding endpoints, gas_budget) justified by the stats below.
4. Largest holders: a short table of the biggest balances per asset from the sample.
Do not invent numbers that are not in the data.

RUN SUMMARY (JSON):
{summary}

LARGEST BALANCES SAMPLE (CSV: address,chain,asset,balance):
{top}
"""


class AnalystAgent:
    def __init__(self, acfg, run_dir):
        self.c = acfg
        self.dir = run_dir

    def _top(self, k=15):
        best = {}
        p = os.path.join(self.dir, "funded.csv")
        if not os.path.exists(p):
            return ""
        with open(p) as f:
            for row in csv.DictReader(f):
                key = (row["chain"], row["asset"])
                lst = best.setdefault(key, [])
                lst.append((int(row["balance_raw"]), row))
                if len(lst) > k * 4:
                    lst.sort(key=lambda x: -x[0])
                    del lst[k:]
        lines = []
        for key, lst in best.items():
            lst.sort(key=lambda x: -x[0])
            for _, r in lst[:k]:
                lines.append(f"{r['address']},{r['chain']},{r['asset']},{r['balance']}")
        return "\n".join(lines)

    def run(self):
        if not self.c.model_id:
            raise SystemExit("analyst.model_id is not set in the config (a Bedrock model or inference profile ID)")
        import boto3
        summary = json.load(open(os.path.join(self.dir, "summary.json")))
        prompt = PROMPT.format(summary=json.dumps(summary, indent=1, default=str)[:60000], top=self._top())
        client = boto3.client("bedrock-runtime", region_name=self.c.region)
        log(f"analyst: asking {self.c.model_id} for a run report")
        resp = client.converse(modelId=self.c.model_id,
                               messages=[{"role": "user", "content": [{"text": prompt}]}],
                               inferenceConfig={"maxTokens": self.c.max_tokens, "temperature": 0.2})
        text = "".join(b.get("text", "") for b in resp["output"]["message"]["content"])
        path = os.path.join(self.dir, "report.md")
        with open(path, "w") as f:
            f.write(text)
        log(f"analyst: wrote {path}")
        return path
