# 部署与发布

`uv-im-connector` 作为独立 IM infrastructure 服务运行。调用方应用通过 HTTP/WebSocket 连接它，不负责启动、升级或托管 provider 长连接。

## 发布产物

每个 `v*` tag 发布三类产物：

| 产物 | 用途 |
| --- | --- |
| GitHub Release tarball | 在 VM、裸机或 systemd 环境安装独立二进制。 |
| `ghcr.io/hengshi/uv-im-connector:<tag>` | Docker、Compose、Kubernetes 和其他容器平台部署。 |
| Go module tag | Go 调用方复用协议类型、client、provider contract 或嵌入式 server。 |

二进制和容器镜像都会写入 build metadata。调用方可以通过 `/v1/meta` 读取当前服务版本和协议版本。

## 版本契约

`GET /v1/meta` 返回服务自描述信息：

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

调用方启动时应该检查：

- `service == "uv-im-connector"`；
- `protocol_version` 属于调用方支持的协议集合；
- 所需 provider/connector 存在；
- 所需能力在 `capabilities` 中为 true。

同一 `protocol_version` 内的 bugfix 可以只升级 `uv-im-connector` 服务。只有协议不兼容、调用方要使用新的 Go client/API，或调用方自身接入逻辑变化时，调用方应用才需要发版。

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

使用仓库内的 Compose 示例：

```bash
cp examples/uv-im-connector.env.example .env
export UV_IM_CONNECTOR_VERSION=<tag>
docker compose -f deploy/compose.yaml up -d
```

## systemd

1. 下载 release tarball，安装到 `/usr/local/bin/uv-im-connector`。
2. 创建运行用户和状态目录。
3. 复制 `examples/uv-im-connector.env.example` 到 `/etc/uv-im-connector/uv-im-connector.env` 并填写 provider credentials。
4. 复制 `deploy/systemd/uv-im-connector.service` 到 `/etc/systemd/system/uv-im-connector.service`。

```bash
sudo useradd --system --home /var/lib/uv-im-connector --shell /usr/sbin/nologin uvim
sudo install -d -o uvim -g uvim /var/lib/uv-im-connector /etc/uv-im-connector
sudo systemctl daemon-reload
sudo systemctl enable --now uv-im-connector
```

## Kubernetes

`deploy/kubernetes/uv-im-connector.yaml` 提供基础 Deployment、Service、ConfigMap、Secret 和状态卷示例。生产环境应该：

- 把 Secret 改为集群的密钥管理方案；
- 使用 PersistentVolumeClaim 保存 event log 和 resource store；示例 manifest 通过 `runAsUser/runAsGroup/fsGroup=10001` 让非 root 容器可以写入挂载卷；
- 只在集群内暴露 Service，或放到受保护的 ingress/reverse proxy 后；
- 让调用方应用使用 `http://uv-im-connector:8787` 作为 connector URL。

## 升级

二进制部署：

```bash
sudo systemctl stop uv-im-connector
sudo install -m 0755 uv-im-connector /usr/local/bin/uv-im-connector
sudo systemctl start uv-im-connector
curl -H "Authorization: Bearer $UV_IM_AUTH_TOKEN" http://127.0.0.1:8787/v1/meta
```

容器部署：

```bash
export UV_IM_CONNECTOR_VERSION=<tag>
docker compose -f deploy/compose.yaml pull
docker compose -f deploy/compose.yaml up -d
curl -H "Authorization: Bearer $UV_IM_AUTH_TOKEN" http://127.0.0.1:8787/v1/meta
```

升级后以 `/health` 判断进程存活，以 `/v1/meta` 判断服务版本、协议版本和 provider 能力。
