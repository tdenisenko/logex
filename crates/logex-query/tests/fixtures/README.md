# Mainnet query regression rows

`mainnet-exact-sums.json` contains 64 unmodified Ethereum mainnet event projections
selected from WETH Deposit/Withdrawal, USDC Mint/Burn and Compound V3
SupplyCollateral captures. It includes original block/transaction hashes and log
indices. Non-timestamp fields were compared with `eth_getLogs`; timestamps came
from LogEx's stored block metadata. `source: Receipt` is assigned from the verified
receipt-only ingestion provenance, because the capture projection omitted it.

The file records capture hashes. Complete capture files are kept in the local
benchmark archive outside Git, not required for the regression tests. This subset
does not establish complete token history or wallet balances.

Expected groups were calculated independently in Python by grouping the captured
rows on the named column and summing `int(row['data'], 16)`. `even` sums only rows
whose original `log_index` is even, returning null for an empty selection; `odd`
sums the remaining rows and uses zero for an empty selection. No Ethereum value
was changed to create edge cases. The tests exercise nullable topics, values above
u64, CASE, HAVING, numeric ordering, pagination, memory rejection and cancellation
across raw, compacted, indexed and reopened storage.

The scan-planning tests reuse these same rows to check exact address/topic
disjunctions, cumulative UNION candidate counts, and cleanup after bounded
parallel selection encounters capacity limits or cancellation. No further
benchmark rows are generated.

`mainnet-transfer-filters.json` adds two unchanged LINK/UNI Transfer events from
the full-range wallet capture, with their original RPC/SQL capture hashes. The
bloom regressions combine them with the rows above to check multiple emitters,
event alternatives and conservative fallback for events absent from the index
format. Protocol-specific Mint/Burn events are not standard Transfer events.
