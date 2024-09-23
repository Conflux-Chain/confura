# Confura

Implementation of an Ethereum Infura equivalent public RPC service on Conflux Network.

[![MIT licensed][1]][2]

[1]: https://img.shields.io/badge/license-MIT-blue.svg
[2]: LICENSE

### Why Confura

Comparatively running your full node, Confura makes it easy to build a high performance, scalable and available RPC service by providing some augmented features.

#### RPC Improvement

- Expiry cache for some high frequency RPC methods such as `cfx_getStatus` and `cfx_epochNumber`.
- Off-chain index of event logs, by which *getLog* (both `cfx_getLogs` and `eth_getLogs`) are handled rather than directly by a full node. This index is backed by a traditional database, which allows us to index and query on more data, without the added overhead of false positives experienced with a bloom filter on full node. All event logs (with total amount less than 10,000) of some contract can even be retrieved within single request.
- Shared proxy subscription for Pub/Sub per full node hence more concurrent sessions supported are possible.
- Improvements over the standard filter APIs by migrating the storage of filter "state" (only event logs for now) out of the full node into memory and database of a new backend system we've dubbed "Virtual Filters" so that a more reliable, high performance and more customizable (eg., long polling timeout for filter changes) filter APIs can be achieved.

#### Node Cluster Management

- Health monitoring to eliminate unhealthy nodes of which latest block height lags behind the overall average, or heartbeat RPC failures or timeout limit exceeded.
- Consistent hashing load balancing by remote IP address.
- Workloads isolation by dedicated node pools.
- JSON-RPC to manage (add/list/delete) node.

#### Rate Limit

- Command line toolset to add/delete/manage custom rate limit strategy and API key.
- Support to rate limit per RPC method with *fixed window* or *token bucket* algorithm.

#### VIP Support

- Billing payment or VIP subscription with our decentralized [Web3 Payment Service](https://github.com/Conflux-Chain/web3pay-service).

#### Metrics

- Component instrumentation && monitoring using [RED](https://www.weave.works/blog/the-red-method-key-metrics-for-microservices-architecture/) method.

#### EVM Compatibility

- Verified and tested across numerous prominent EVM-compatible blockchains, platforms, and Layer 2 solutions.

## Building the source

Building Confura requires a Go (version 1.22 or later) compiler. Once the dependencies are installed, run

```shell
go build -o bin/confura
```

or if you are using Unix-based OS such as Mac or Linux, you can leverage make tool:

```shell
make build
```

An executable binary named *confura* will be generated in the project *bin* directory.

## Running Confura

Confura is comprised of serveral components as below:

* Blockchain Sync *(synchronizes blockchain data with persistent storage or cache)*
* Node Management *(manages full node clusters including health monitoring and traffic routing etc.)*
* Virtual Filter *(polls filter changes instantly into storage, also acts as a proxy to serves filter API requests)*
* RPC Proxy *(optimizes certain RPC methods, particularly `cfx/eth_getLogs` to serve by responding directly from storage)*
* Data Validator *(constantly scrapes blockchain data from both full node and RPC Proxy for validation comparision)*

### Blockchain Sync

You can use the `sync` subcommand to start sync service:

> Usage:
>   ./confura sync [flags]
>
> Flags:
>
>       --db    start core space DB sync server
>       --eth   start EVM space DB sync server
>       --help  help for sync

eg., run the following to start synchronizing core space blockchain data into database:

```shell
$ ./confura sync --db
```

### Node Management

You can use the `nm` subcommand to start node management service:

> Usage:
>  ./confura nm [flags]
>
> Flags:
>
>      --cfx  start core space node manager server
>      --eth  start EVM space node manager server
>      --help help for nm

eg., run the following for core space node manager server:

```shell
$ ./confura nm --cfx
```

### Virtual Filter

You can use the `vf` subcommand to start virtual filter service:

> Usage:
>  ./confura vf [flags]
>
> Flags:
>
>      --cfx  start core space virtual filter server
>      --eth  start EVM space virtual filter server
>      --help help for nm

eg., run the following for core space virtual filter server:

```shell
$ ./confura vf --cfx
```

### RPC Proxy

You can use the `rpc` subcommand to start RPC proxy servers:

> Usage:
>  ./confura rpc [flags]
>
> Flags:
>
>      --cfx        start core space RPC server
>      --cfxBridge  start core space bridge RPC server
>      --eth        start evm space RPC server
>      --help       help for rpc

eg., run the following for core space RPC server:

```shell
$ ./confura rpc --cfx
```

### Data Validator

You can use the `test` subcommand to start the data consistency testing:

> Usage:
>  ./confura test [command] [flags]
>
> Available Commands:
>
>       cfx validate epoch data from Core Space JSON-RPC proxy against the fullnode
>       eth validate block data from EVM Space JSON-RPC proxy against the fullnode
>       ws  validate epoch/block data from Pub/Sub proxy against the fullnode
>       vf  validate filter changes polled from Virtual-Filter proxy against the fullnode
>
> Flags: Use `./confura test [command] --help` to see a list of available flags for each command.

eg., run the following to validate data for the Core Space JSON-RPC proxy.

```shell
$ ./confura test cfx --fn-endpoint http://test.confluxrpc.com --infura-endpoint http://127.0.0.1:22537
```

## Deployment

For deployment, please refer to our [Deployment Guide](./doc/DEPLOY.md).

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