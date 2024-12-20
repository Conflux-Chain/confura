# Grafana Dashboard

This folder contains all the dashboard JSON models, which are compatible with Grafana version 11.2.2 (the version we use). They may also work with other versions, but compatibility is not guaranteed.
| Json Model          	| Description                               	|
|---------------------	|-------------------------------------------	|
| RPC-RED-Overview    	| `RED` metrics overview for RPC proxy        	|
| RPC-RED-Drilldown   	| `RED` metrics drilldown for RPC proxy       	|
| RPC-Input-Breakdown 	| Input parameter breakdown for RPC proxy   	|
| RPC-Batch           	| JSON-RPC batch requests for RPC proxy     	|
| RPC-PubSub          	| Websocket Pub/Sub metrics for RPC proxy   	|
| RPC-Fullnode-Proxy  	| Full node proxy metrics for RPC proxy     	|
| RPC-Store-Ops       	| Store operation metrics for RPC proxy     	|
| Node-Management     	| Node cluster management metrics           	|
| Sync-RED            	| `RED` metrics for indexing sync             	|
| Sync-Fullnode-Query 	| Full node query metrics for indexing sync 	|
| Sync-Store-Ops      	| Store operation metrics for indexing sync 	|
| Virtual-Filter      	| Operation (eg., polling) metrics for filter 	|

*`RED`: Three key metrics are collected: `Rate`, `Error` and `Duration`.*
> **Note**: Any InfluxDB data sources created in Grafana must have a name starting with the `confura_*` prefix for proper integration with these dashboards.
