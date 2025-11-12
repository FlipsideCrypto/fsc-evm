{% docs ez_protocol_metrics_table_doc %}

## What

Daily aggregated protocol-level metrics for the indicated EVM blockchain including user activity, transaction counts, and token flow data. Combines protocol interaction data with Flipside scoring to distinguish between total activity and quality user activity (Flipside score >= 4). Metrics track inflows, outflows, and user engagement at the protocol level.

{% enddocs %}

{% docs ez_protocol_metrics_day_ %}

The date in YYYY-MM-DD format - all stats are aggregated at the daily level

{% enddocs %}

{% docs ez_protocol_metrics_protocol %}

Protocol names including versioning information (e.g. Uniswap v2, v3, etc.). Note: protocol names may not always match protocol names in other tables like dex_volume

{% enddocs %}

{% docs ez_protocol_metrics_n_users %}

Number of unique addresses submitting a transaction that interacts with any protocol contract. Same address can be counted across different protocols, but never more than once within a single protocol

{% enddocs %}

{% docs ez_protocol_metrics_n_quality_users %}

Number of unique quality addresses with Flipside score >= 4 submitting a transaction that interacts with any protocol contract. Same address can be counted across different protocols, but never more than once within a single protocol

{% enddocs %}

{% docs ez_protocol_metrics_n_transactions %}

Number of unique transactions that emit 1 or more events from any protocol contract. Same transaction can be counted across different protocols, but never more than once within a single protocol

{% enddocs %}

{% docs ez_protocol_metrics_n_quality_transactions %}

Number of unique transactions by addresses with Flipside score >= 4 that emit 1 or more events from any protocol contract. Same transaction can be counted across different protocols, but never more than once within a single protocol

{% enddocs %}

{% docs ez_protocol_metrics_usd_inflows %}

USD value of tokens sent INTO this protocol from all other addresses/protocols/contracts (excludes transfers within the same protocol)

{% enddocs %}

{% docs ez_protocol_metrics_usd_outflows %}

USD value of tokens sent FROM this protocol to all other addresses/protocols/contracts (excludes transfers within the same protocol)

{% enddocs %}

{% docs ez_protocol_metrics_net_usd_inflow %}

USD inflows minus outflows. Note: Price effects may cause net USD and token-level net changes to differ in direction (rare but possible with large, fast price changes)

{% enddocs %}

{% docs ez_protocol_metrics_gross_usd_volume %}

Total USD volume calculated as inflows plus outflows

{% enddocs %}

{% docs ez_protocol_metrics_quality_usd_inflows %}

USD value of token inflows from quality addresses with Flipside score >= 4

{% enddocs %}

{% docs ez_protocol_metrics_quality_usd_outflows %}

USD value of token outflows to quality addresses with Flipside score >= 4

{% enddocs %}

{% docs ez_protocol_metrics_quality_net_usd %}

Quality USD inflows minus quality USD outflows. Price effects may apply similar to net_usd_inflow

{% enddocs %}

{% docs ez_protocol_metrics_quality_gross_usd %}

Total quality USD volume calculated as quality inflows plus quality outflows

{% enddocs %}

