
# Deployment Guide

Before deploying, make sure you understand our [System Architecture](ARCHITECTURE.md).

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

* MySQL Server v5.7+ *(used for off-chain persistent storage)*
* Redis Server v6.2+ *(optional, used for off-chain cache)*
* InfluxDB v1.x *(optional, metrics storage)*
* Grafana v8.0.x+ *(optional, metrics dashboard)*

## Configuration

Confura loads configurations from `config.yml` or `config/config.yml` in the current directory at startup. You can use the provided [config.yml](../config/config.yml) as a starting template, which includes helpful comments to assist you in customizing it according to your needs.

If you prefer not to modify individual settings in the configuration file, you can use environment variables instead. Environment variables prefixed with `INFURA_` will override the corresponding configuration items, where the path in the config file is represented by underscores.

For example:

```shell
export INFURA_RPC_ENDPOINT=":32537"
```

This command will override the `rpc.endpoint` value in the configuration file with ":32537".

## Docker Quick Start

Follow these steps to get Confura up using Docker:

1. Build the Docker Image

Run the following command to build a lean confura Docker image based on Alpine Linux:

```shell
$ docker-compose build
```

2. Start the Containers

Start the MySQL, Influxdb and Grafana containers along with the Confura components:

```shell
$ docker-compose up -d
```

3. Check Container Status

Verify that all containers are running smoothly with:

```shell
$ docker-compose ps
```

## Misc

### Load Balancer

To ensure that the real IP address is correctly passed to the Node Manager for consistent IP hash-based routing, configure your load balancer to use the `X-Forwarded-For` or `X-Real-IP` headers. Additionally, apply IP hash routing in the load balancer to maintain consistent client-to-RPC proxy routing.

If you’re using Nginx, you can use the following configuration as an example:

```nginx
http {
    upstream rpc_proxy_backend {
        ip_hash;  
        ...
    }

    server {
        listen 80;

        location / {
            proxy_pass http://rpc_proxy_backend;
            proxy_set_header X-Forwarded-For $http_x_forwarded_for;
            proxy_set_header X-Real-IP $remote_addr;
        }
    }
}
```

### Grafana Dashboard

After setting up InfluxDB, you can access a Grafana dashboard to visualize the collected metrics. The dashboard is pre-configured to display key metrics such as request rates, latency, and error rates.

To set up Grafana, follow these steps:

1.	Configure InfluxDB as a Data Source:
- Open the Grafana web interface.
- Go to the data sources section and add InfluxDB as a data source.
- Name the data source `infura_testnet` or `infura_mainet`, depending on your environment.

2. Import the Dashboard Configuration:
- Refer to the [Grafana Dashboard](../grafana/README.md) for the provided dashboard configuration files.
- In Grafana, go to the “Create” tab, select “Import,” and upload the JSON configuration file to set up the dashboard.

