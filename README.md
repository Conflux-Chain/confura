# Confura

Implementation of an Ethereum Infura equivalent public RPC service on Conflux Network.

[![MIT licensed][1]][2]

[1]: https://img.shields.io/badge/license-MIT-blue.svg
[2]: LICENSE

### Why Confura

Comparatively running your full node, Confura makes it easy to build a high performance, scalable and available RPC service by providing some augmented features.

#### RPC Improvement

- Expiry cache for some high frequency RPC methods such as `cfx_getStatus` and `cfx_epochNumber`.
- Off-chain index of event logs, by which `getLogs` (both `cfx_getLogs` and `eth_getLogs`) are handled rather than directly by a full node. This index is backed by a traditional database, which allows us to index and query on more data, without the added overhead of false positives experienced with a bloom filter on full node. All event logs (with total amount less than 10,000) of some contract can even be retrieved within single request.
- Shared proxy subscription for Pub/Sub per full node hence more concurrent sessions supported are possible.
- Improvements over the standard filter APIs by migrating the storage of filter "state" (only event logs for now) out of the full node into memory and database of a new backend system we've dubbed "Virtual Filters" so that a more reliable, high performance and more customizable (eg., long polling timeout for filter changes) filter APIs can be achieved.

#### Node Cluster Management

- Health monitoring to eliminate unhealthy nodes of which latest block height lags behind the overall average, or heartbeat RPC failures or timeout limit exceeded.
- Consistent hashing load balancing by remote IP address.
- Workloads isolation by dedicated node pools.
- JSON-RPC to manage (add/list/delete) node.

#### Rate Limit

- Command line toolset to add/delete/manage custom rate limit strategy and API key.
- Support to rate limit per RPC method with `fixed window` or `token bucket` algorithm.

#### VIP Support

- Billing payment or VIP subscription with our decentralized [Web3 Payment Service](https://github.com/Conflux-Chain/web3pay-service).

#### Metrics

* Component instrumentation && monitoring using [RED](https://www.weave.works/blog/the-red-method-key-metrics-for-microservices-architecture/) method.

## Prerequisites

### Hardware Requirements

Minimum:

* CPU with 2+ cores
* 4GB RAM
* 200GB (with prune) or otherwise 1TB free storage space to sync the Mainnet/Testnet
* 8 MBit/sec download Internet service

Recommended:

* Fast CPU with 4+ cores
* 8GB+ RAM
* High Performance SSD with at least 1TB free space
* 25+ MBit/sec download Internet service

### Software Requirements

Confura can even be run without any third party dependencies. But depending on what you plan to do with it, you will require the following:

* MySQL Server v5.7+ *(mainly used for off-chain persistent storage)*
* Redis Server v6.2+ *(mainly used for off-chain cache storage)*
* InfluxDB v1.x *(metrics storage)*
* Grafana v8.0.x+ *(metrics dashboard)*

## Building the source

Building Confura requires a Go (version 1.15 or later) compiler. Once the dependencies are installed, run

```shell
go build -o bin/confura
```

or if you are using Unix-based OS such as Mac or Linux, you can leverage make tool:

```shell
make build
```

An executable binary named *`confura`* will be generated in the project *`bin`* directory.

## Configuration

Confura will load configurations from `config.yml` or `config/config.yml` under current directory at startup. You can use the [config.yml](config/config.yml) within our project as basic template, which has bunch of helpful comments to make it easy to customize accordingly to your needs.

As an alternative to modifying the numerous config items in the configuration file, you can also use environment variables. Environment variables prefixed `"INFURA_"` will try to overide the config item defined by the path with all its parts split by underscore.

eg.,
```shell
export INFURA_RPC_ENDPOINT=":32537"
```
This will override value for the config item of path `rpc.endpoint` within the configuration file as `":32537"`.

## Running Confura

Confura is comprised of serveral components as below:

* Blockchain Sync *(synchronizes blockchain data to persistent storage or memory cache)*
* Node Management *(manages full node clusters including health monitoring and routing etc.)*
* Virtual Filter *(polls filter changes instantly into cache and persistent storage, also acts as a proxy to serves filter API requests)*
* RPC Proxy *(optimizes some RPC methods (especially *`cfx/eth_getLogs`*) to serve by responding directly from cache or persistent storage)*
* Data Validator *(constantly scrapes blockchain data from both full node and RPC Proxy for validation comparision)*

### Blockchain Sync

You can use the `sync` subcommand to start sync service, including DB/KV/ETH sync as well as fast catchup.

> Usage:
>   confura sync [flags]
>
> Flags:
>
>       --db           start core space DB sync server
>       --eth          start ETH sync server
>       --kv           start core space KV sync server
>       --catchup      start core space fast catchup server
>       --adaptive     automatically adjust target epoch number to the latest stable epoch
>       --benchmark    benchmarking the performance during fast catch-up sync (default true)
>       --start uint   the epoch from which fast catch-up sync will start
>       --end uint     the epoch until which fast catch-up sync will end
>       --help         help for sync

eg., you can run the following to kick off synchronizing core space blockchain data into database:

```shell
$ confura sync --db
```

*Note: You may need to prepare for the configuration before you start the service.*

### Node Management

You can use the `nm` subcommand to start node management service for core space or evm space.

> Usage:
>  confura nm [flags]
>
> Flags:
>
>      --cfx    start core space node manager server
>      --eth    start evm space node manager server
>      --help   help for nm

eg., you can run the following for core space node manager server:

```shell
$ confura nm --cfx
```

*Note: You may need to prepare for the configuration before you start the service.*

### Virtual Filter

You can use the `vf` subcommand to start virtual filter service (for eSpace only).

> Usage:
>  confura vf [flags]
>
> Flags:
>
>      --help   help for nm

eg., you can run the following for virtual filter server:

```shell
$ confura vf
```

*Note: You may need to prepare for the configuration before you start the service.*

### RPC Proxy

You can use the `rpc` subcommand to start RPC service, including core space, evm space and core space bridge RPC servers.

> Usage:
>  confura rpc [flags]
>
> Flags:
>
>      --cfx         start core space RPC server
>      --cfxBridge   start core space bridge RPC server
>      --eth         start evm space RPC server
>      --help        help for rpc

eg., you can run the following for core space RPC server:

```shell
$ confura rpc --cfx
```

*Note: You may need to prepare for the configuration before you start the service.*

### Data Validator

You can use the `test` subcommand to start data validity test for `JSON-RPC`, `Pub/Sub` or `Filter API` proxy.

> Usage:
>  confura test [command] [flags]
>
> Available Commands:
>
>       cfx         validate if epoch data from core space JSON-RPC proxy complies with fullnode
>       ws          validate if epoch data from core space Pub/Sub proxy complies with fullnode
>       eth         validate if epoch data from evm space JSON-RPC proxy complies with fullnode
>       vf          validate if filter changes polled from Virtual-Filter proxy complies with fullnode
>
> Flags: Use `confura test [command] --help` to list all possible flags for each specific command.

eg., you can run the following to kick off data validity test for core space JSON-RPC proxy.

```shell
$ confura test cfx --fn-endpoint http://test.confluxrpc.com --infura-endpoint http://127.0.0.1:22537
```

*Note: You need to boot up RPC proxy (or Virtual Filter proxy) before you start the validation test.*

### Docker Quick Start

One of the quickest ways to get Confura up and running on your machine is by using Docker Compose:

```shell
$ docker-compose build
```

This will build a slim `confura` executable docker image based on Alpine Linux.

```shell
$ docker-compose up -d
```

This will start `MySQL` and `Redis` dependency middleware containers, for each of which a docker volume is created to persist data even the container is removed. It will also boot up the above components with some basic pre-configurations, and map the default ports as needed.

```shell
$ docker-compose ps 

           Name                         Command               State                      Ports                    
------------------------------------------------------------------------------------------------------------------
confura-data-validator       ./confura test cfx -f http ...   Up                                                  
confura-database             docker-entrypoint.sh mysqld      Up      0.0.0.0:53780->3306/tcp, 33060/tcp          
confura-ethdata-validator    ./confura test eth -f http ...   Up                                                  
confura-ethnode-management   ./confura nm --eth               Up      0.0.0.0:28530->28530/tcp,:::28530->28530/tcp
confura-ethrpc               ./confura rpc --eth              Up      0.0.0.0:28545->28545/tcp,:::28545->28545/tcp
confura-ethsync              ./confura sync --eth             Up                                                  
confura-node-management      ./confura nm --cfx               Up      0.0.0.0:22530->22530/tcp,:::22530->22530/tcp
confura-redis                docker-entrypoint.sh redis ...   Up      0.0.0.0:53779->6379/tcp                     
confura-rpc                  ./confura rpc --cfx              Up      0.0.0.0:22537->22537/tcp,:::22537->22537/tcp
confura-sync                 ./confura sync --db --kv         Up                                                  
confura-virtual-filter       ./confura vf                     Up      0.0.0.0:48545->48545/tcp,:::48545->48545/tcp
```

## TODO

- [ ] Add in-memory cache of event logs for "near head" recent blocks.
- [ ] Release performance benchmark report.

## Contribution


Thank you for considering to help out with the source code! We welcome contributions from anyone on the internet, and are grateful for even the smallest of fixes!

If you'd like to contribute to `confura`, please fork, fix, commit and send a pull request for the maintainers to review and merge into the main code base. If you wish to submit more complex changes though, please file an issue first to ensure those changes are in line with the general philosophy of the project and/or get some early feedback which can make both your efforts much lighter as well as our review and merge procedures quick and simple.

Please make sure your contributions adhere to our coding guidelines:

 * Code must adhere to the official Go [formatting](https://golang.org/doc/effective_go.html#formatting)
   guidelines (i.e. uses [gofmt](https://golang.org/cmd/gofmt/)).
 * Code must be documented adhering to the official Go [commentary](https://golang.org/doc/effective_go.html#commentary)
   guidelines.
 * Pull requests need to be based on and opened against the `main` branch.
 * Commit messages should be prefixed with the package(s) they modify.
   * E.g. "cfx sync: add nearhead memory cache"

## License

This project is licensed under the [MIT License](LICENSE).