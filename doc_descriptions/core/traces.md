{% docs evm_traces_table_doc %}

This table contains flattened trace data for internal contract calls on this EVM blockchain. Hex encoded fields can be decoded to integers by using `TO_NUMBER(<FIELD>, 'XXXXXXXXXXXX')`, with the number of Xs being the max length of the encoded field. You must also remove the `0x` from your field to use the `TO_NUMBER()` function, if applicable. 

{% enddocs %}


{% docs evm_traces_block_no %}

The block number of this transaction.

{% enddocs %}


{% docs evm_traces_blocktime %}

The block timestamp of this transaction.

{% enddocs %}


{% docs evm_traces_call_data %}

The raw JSON data for this trace.

{% enddocs %}


{% docs evm_traces_value %}

The amount of the native asset transferred in this trace.

{% enddocs %}


{% docs evm_traces_from %}

The sending address of this trace. This is not necessarily the from address of the transaction. 

{% enddocs %}


{% docs evm_traces_gas %}

The gas supplied for this trace.

{% enddocs %}


{% docs evm_traces_gas_used %}

The gas used for this trace.

{% enddocs %}


{% docs evm_traces_identifier %}

This field represents the position and type of the trace within the transaction. 

{% enddocs %}


{% docs evm_trace_index %}

The index of the trace within the transaction.

{% enddocs %}


{% docs evm_traces_input %}

The input data for this trace.

{% enddocs %}


{% docs evm_traces_output %}

The output data for this trace.

{% enddocs %}


{% docs evm_traces_sub %}

The amount of nested sub traces for this trace.

{% enddocs %}


{% docs evm_traces_to %}

The receiving address of this trace. This is not necessarily the to address of the transaction. 

{% enddocs %}


{% docs evm_traces_tx_hash %}

The transaction hash for the trace. Please note, this is not necessarily unique in this table as transactions frequently have multiple traces. 

{% enddocs %}


{% docs evm_traces_type %}

The type of internal transaction. Common trace types are `CALL`, `DELEGATECALL`, and `STATICCALL`.

{% enddocs %}
