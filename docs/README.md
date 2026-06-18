# Tether Bridge

两个 Hermes 实例之间的可靠消息通道。

## 目录结构

```
~/.hermes/tether/
├── tether_server.py      # HTTP 服务（收消息、存 DB）
├── tether_watcher.py     # 消息处理守护进程
├── tether_send.py        # CLI 发送工具
├── tether_send_file.py   # 文件传输工具
├── tether_dump.py        # CLI 消息查看工具
├── tether_relay.py       # VPS 中继服务
├── tether_web.py/.html   # Web 展示页面
├── tether_alive.py       # 保活监控守护进程
├── tether.db             # SQLite 消息数据库
├── docs/                 # 设计文档、手册
│   ├── DESIGN.md
│   ├── MANUAL.md
│   └── USAGE.md
├── config/               # systemd 服务文件、配置模板
│   ├── tether.service
│   ├── tether-watcher.service
│   └── config.env.template
└── archive/              # 旧版本存档
```
