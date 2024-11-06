{% docs evm_abis_table_doc %}

This table contains the contract ABIs that we have sourced from blockchain explorers, the community, or bytecode matched. This table is the source of ABIs used in the `core__ez_decoded_event_logs` table.

We first try to source ABIs from blockchain explorers. If we cannot find an ABI from explorers, we will rely on user submissions. To add a contract to this table, please visit [here](https://science.flipsidecrypto.xyz/abi-requestor/).

If we are unable to locate an ABI for a contract from explorers or the community, we will try to find an ABI to use by matching the contract bytecode to a known contract bytecode we do have an ABI for.

{% enddocs %}

{% docs abi_source %}

The source of the ABI. This can be `<explorer_name>`, `user_submitted`, or `bytecode_matched`.

{% enddocs %}

{% docs abi %}

The JSON ABI for the contract.

{% enddocs %}

{% docs bytecode %}

The deployed bytecode of the contract.

{% enddocs %}