# Tether Bridge — MacBook 部署指南

## 前提
- MacBook Tailscale 已通（IP: 100.81.192.38）
- Hermes 已在 MacBook 上运行（mac-小钉hermes）

## 部署步骤

把下面这段话发给 **mac-小钉hermes** 执行：

```
mkdir -p ~/.hermes/tether && python3 -m venv ~/.hermes/tether/venv && ~/.hermes/tether/venv/bin/pip install flask && echo "✅ Flask installed"
```

然后，把 `tether_server.py` 和 `tether_client.py` 传到 MacBook 上。两个方法：

### 方法 A：让 Hermes 直接写文件（推荐）

对 mac-小钉hermes 说：

> 请在 ~/.hermes/tether/ 下创建 tether_server.py，文件内容如下：
> [粘贴下面 ThinkPad 上的 tether_server.py 内容]

> 再创建 tether_client.py，内容如下：
> [粘贴 ThinkPad 上的 tether_client.py 内容]

### 方法 B：通过 USB 或共享目录拷贝

1. 从 ThinkPad 的 `~/.hermes/tether/` 复制两个 Python 文件
2. 传到 MacBook 的 `~/.hermes/tether/`

或者：我直接把两个文件的内容发给你，你手动粘贴。

## 后续配置

创建 CLI 命令：

```
chmod +x ~/.hermes/tether/tether_server.py ~/.hermes/tether/tether_client.py
ln -sf ~/.hermes/tether/tether_client.py ~/.local/bin/tether
```

设置 Token（跟 ThinkPad 一样的密钥）：

```
echo 'TETHER_TOKEN=6c84e624e6cae2b760b4d4ed7bd7f2f8' >> ~/.hermes/.env
```

安装 systemd 服务：

```
sudo tee /etc/systemd/system/tether.service > /dev/null << 'EOF'
[Unit]
Description=Tether Bridge — Hermes 实例间通信
After=network-online.target tailscaled.service
Wants=network-online.target

[Service]
Type=simple
User=zzsky
ExecStart=/home/zzsky/.hermes/tether/tether_server.py --port 9001
Restart=always
RestartSec=5
Environment=TETHER_TOKEN=6c84e624e6cae2b760b4d4ed7bd7f2f8
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now tether.service
sudo systemctl status tether.service --no-pager
```

## 验证

### 在 MacBook 上 ping ThinkPad

```
tether ping 100.102.54.90
```

### 从 ThinkPad ping MacBook

（待 MacBook Tether 跑通后）

```
tether ping 100.81.192.38
```

### 发消息测试

```
tether msg 100.102.54.90 "MacBook 通过 Tether 打招呼!"
```

## Token

两台机器使用同一个 Token：`6c84e624e6cae2b760b4d4ed7bd7f2f8`

已保存在：
- ThinkPad: `~/.hermes/.env`（`TETHER_TOKEN` 环境变量）
- 服务文件: `/etc/systemd/system/tether.service`（`Environment=TETHER_TOKEN=...`）
