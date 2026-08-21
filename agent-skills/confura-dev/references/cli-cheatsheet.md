# CLI Cheatsheet

Use commands from the built `confura` binary. Prefer explicit subcommands for clarity.

## Build And Version

```shell
make build
./bin/confura --version
```

Direct build:

```shell
go build -o bin/confura
```

## Service Commands

Core Space:

```shell
./bin/confura sync --db
./bin/confura nm --cfx
./bin/confura vf --cfx
./bin/confura rpc --cfx
```

eSpace:

```shell
./bin/confura sync --eth
./bin/confura nm --eth
./bin/confura vf --eth
./bin/confura rpc --eth
```

CfxBridge:

```shell
./bin/confura rpc --cfxBridge
```

Trace log sync:

```shell
./bin/confura sync --trace
```

## Validators

Core Space JSON-RPC:

```shell
./bin/confura test cfx --fn-endpoint <fullnode_rpc> --infura-endpoint <confura_rpc>
```

eSpace JSON-RPC:

```shell
./bin/confura test eth --fn-endpoint <fullnode_rpc> --infura-endpoint <confura_rpc>
```

Websocket Pub/Sub:

```shell
./bin/confura test ws --network cfx --fn-endpoint <fullnode_ws> --infura-endpoint <confura_ws>
```

Virtual Filter:

```shell
./bin/confura test vf --network cfx --fn-endpoint <fullnode_rpc> --infura-endpoint <virtual_filter_rpc>
```

Check exact flags with `./bin/confura test <command> --help`.

## Admin Utilities

Rate limit strategy examples:

```shell
./bin/confura ratelimit lss --network cfx
./bin/confura ratelimit adds --network cfx --name <strategy> --rules '<json_rules>'
./bin/confura ratelimit rms --network cfx --name <strategy>
```

Rate limit key examples:

```shell
./bin/confura ratelimit gk --type 0
./bin/confura ratelimit addk --network cfx --strategy <strategy> --key <key> --type 0 --memo <memo>
./bin/confura ratelimit lsk --network cfx --strategy <strategy>
./bin/confura ratelimit rmk --network cfx --key <key>
```

Access control allowlist examples:

```shell
./bin/confura acl lsal --network cfx
./bin/confura acl addal --network cfx --name <allowlist> --rules '<json_rules>'
./bin/confura acl rmal --network cfx --name <allowlist>
```

Node route examples:

```shell
./bin/confura noderoute ls --network cfx
./bin/confura noderoute add --network cfx --key <route_key> --group <route_group>
./bin/confura noderoute rm --network cfx --key <route_key>
```

Use `--network eth` for eSpace admin data where supported.

## Docker

```shell
docker-compose build
docker-compose up -d
docker-compose ps
```

The repository documentation uses `docker-compose`. In environments standardized on Compose v2, `docker compose` may be used if available.
