# leanSPHINCS test vector

`verify_valid_known_answer.yaml` is `tests/cryptography/sphincs/verify/verify_valid_known_answer/data.yaml` of [ethereum/EthLib](https://github.com/ethereum/EthLib) at commit `f0e41b61ed6349f7ae35e8a4528c7da847bf27a8`, the commit that the EIP-8141 `PROOF` frame draft (ethereum/EIPs#12714) pins for `LEANSPHINCS_VERIFIER`.
It is copied unchanged; EthLib's `SHA256SUMS` gives it the sha256 `e7507b8190ba85f8427ee73a4c91aa9a983f99545b552cd4ba0cf222b2e8b0cf`.
EthLib is released under CC0-1.0.

leanVM runs every EthLib vector against the code that lean-pq binds, so Besu keeps only this one, to check its own wrapper and to fail if lean-pq stops matching the pinned specification.
