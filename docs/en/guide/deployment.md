# Deployment and Releases

`uv-im-connector` runs as standalone IM infrastructure. Caller applications connect to it over HTTP/WebSocket and do not start, upgrade, or host provider long connections.

## Release Artifacts

Every `v*` tag publishes three artifact types:

| Artifact | Use |
| --- | --- |
| GitHub Release tarball | Install the standalone binary on a VM, bare-metal host, or systemd service. |
| `ghcr.io/hengshi/uv-im-connector:<tag>` | Deploy with Docker, Compose, Kubernetes, or another container platform. |
| Go module tag | Reuse protocol types, the client, provider contracts, or embedded server packages from Go. |

Binaries and container images include build metadata. Callers can read the current service version and protocol version from `/v1/meta`.

## Version Contract

`GET /v1/meta` returns service metadata:

```json
{
  "service": "uv-im-connector",
  "connector_version": "v0.0.4",
  "protocol_version": "v1",
  "git_commit": "0123456789abcdef",
  "build_time": "2026-07-06T08:00:00Z",
  "providers": [
    {
      "provider": "wecom",
      "connector": "main",
      "capabilities": {
        "inbound": true,
        "outbound": true,
        "group_message": true,
        "download_resource": true
      },
      "health": {
        "provider": "wecom",
        "connector": "main",
        "state": "ok"
      }
    }
  ]
}
```

Caller applications should check:

- `service == "uv-im-connector"`;
- `protocol_version` is in the caller's supported protocol set;
- required provider/connector pairs exist;
- required capabilities are true.

Bugfix releases within the same `protocol_version` can be deployed by upgrading only the `uv-im-connector` service. Caller applications need their own release only when the protocol is incompatible, when they consume new Go client/API surface, or when their integration logic changes.

## Docker

```bash
docker run --rm \
  -p 127.0.0.1:8787:8787 \
  -v uv-im-connector-state:/var/lib/uv-im-connector \
  -e UV_IM_AUTH_TOKEN=change-me \
  -e UV_IM_PROVIDERS=wecom,lark \
  -e UV_WECOM_BOT_ID=... \
  -e UV_WECOM_BOT_SECRET=... \
  -e UV_LARK_APP_ID=... \
  -e UV_LARK_APP_SECRET=... \
  ghcr.io/hengshi/uv-im-connector:<tag>
```

Use the Compose example from this repository:

```bash
cp examples/uv-im-connector.env.example .env
export UV_IM_CONNECTOR_VERSION=<tag>
docker compose -f deploy/compose.yaml up -d
```

## systemd

1. Download the release tarball and install the binary to `/usr/local/bin/uv-im-connector`.
2. Create the runtime user and state directory.
3. Copy `examples/uv-im-connector.env.example` to `/etc/uv-im-connector/uv-im-connector.env` and fill provider credentials.
4. Copy `deploy/systemd/uv-im-connector.service` to `/etc/systemd/system/uv-im-connector.service`.

```bash
sudo useradd --system --home /var/lib/uv-im-connector --shell /usr/sbin/nologin uvim
sudo install -d -o uvim -g uvim /var/lib/uv-im-connector /etc/uv-im-connector
sudo systemctl daemon-reload
sudo systemctl enable --now uv-im-connector
```

## Kubernetes

`deploy/kubernetes/uv-im-connector.yaml` provides a baseline Deployment, Service, ConfigMap, Secret, and state volume example. Production deployments should:

- replace the Secret with the cluster's secret-management mechanism;
- use a PersistentVolumeClaim for the event log and resource store; the sample manifest sets `runAsUser/runAsGroup/fsGroup=10001` so the non-root container can write mounted volumes;
- expose the Service only inside the cluster, or behind a protected ingress/reverse proxy;
- configure caller applications with `http://uv-im-connector:8787` as the connector URL.

## Upgrade

Binary deployment:

```bash
sudo systemctl stop uv-im-connector
sudo install -m 0755 uv-im-connector /usr/local/bin/uv-im-connector
sudo systemctl start uv-im-connector
curl -H "Authorization: Bearer $UV_IM_AUTH_TOKEN" http://127.0.0.1:8787/v1/meta
```

Container deployment:

```bash
export UV_IM_CONNECTOR_VERSION=<tag>
docker compose -f deploy/compose.yaml pull
docker compose -f deploy/compose.yaml up -d
curl -H "Authorization: Bearer $UV_IM_AUTH_TOKEN" http://127.0.0.1:8787/v1/meta
```

After an upgrade, use `/health` for process liveness and `/v1/meta` for service version, protocol version, and provider capabilities.
