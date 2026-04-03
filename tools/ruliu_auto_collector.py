#!/usr/bin/env python3
"""
如流（百度 IM）自动采集器

如流没有公开的消息历史 API，采用两条路径：
  路径 A：Webhook 实时采集（推荐，持续收集）
    启动本地 HTTP server 接收如流自定义机器人 Webhook 推送的消息
  路径 B：导出文件解析（手动，一次性导入）
    解析如流聊天记录导出文件（TXT/JSON），复用 ruliu_parser.py

前置：
  python3 ruliu_auto_collector.py --setup   # 配置（一次性）

用法：
  # 启动 Webhook 接收服务（后台运行，持续采集）
  python3 ruliu_auto_collector.py --start-server --port 8765

  # 导出为标准格式
  python3 ruliu_auto_collector.py --export --target "张三" --output-dir ./knowledge/zhangsan

  # 停止服务
  python3 ruliu_auto_collector.py --stop

  # 解析导出文件（路径 B）
  python3 ruliu_auto_collector.py --parse-file ./chat_export.txt --target "张三" --output-dir ./knowledge/zhangsan
"""

from __future__ import annotations

import json
import sys
import os
import signal
import argparse
import hashlib
import hmac
import base64
from pathlib import Path
from datetime import datetime
from typing import Optional
from threading import Thread

try:
    import requests
except ImportError:
    print("错误：请先安装 requests：pip3 install requests", file=sys.stderr)
    sys.exit(1)

try:
    from flask import Flask, request as flask_request, jsonify
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


CONFIG_PATH = Path.home() / ".colleague-skill" / "ruliu_config.json"
PID_PATH = Path.home() / ".colleague-skill" / "ruliu_server.pid"


# ─── 配置 ────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        print("未找到配置，请先运行：python3 ruliu_auto_collector.py --setup", file=sys.stderr)
        sys.exit(1)
    return json.loads(CONFIG_PATH.read_text())


def save_config(config: dict) -> None:
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(config, indent=2, ensure_ascii=False))


def setup_config() -> None:
    print("=== 如流自动采集配置 ===\n")
    print("如流没有公开的消息历史 API，本工具通过两种方式采集消息：\n")
    print("  路径 A：Webhook 实时采集（推荐）")
    print("    在如流群中添加自定义机器人，配置 Webhook 回调地址")
    print("    本工具启动 HTTP 服务接收推送消息，实时写入本地文件\n")
    print("  路径 B：手动导出")
    print("    从如流客户端导出聊天记录（TXT/JSON），用本工具解析\n")
    print("─── Webhook 配置 ───")
    print("1. 打开如流 → 进入目标群聊 → 群设置 → 智能群助手 → 添加自定义机器人")
    print("2. 获取 Webhook URL（用于接收消息推送的回调地址）")
    print("3. 配置签名密钥（可选，建议开启）\n")

    webhook_secret = input("Webhook 签名密钥（留空跳过）：").strip()
    target_name = input("默认目标用户的如流昵称（留空跳过）：").strip()

    port_str = input("Webhook 服务端口（默认 8765）：").strip()
    port = int(port_str) if port_str else 8765

    output_dir = input("默认输出目录（默认 ./knowledge/ruliu）：").strip()
    if not output_dir:
        output_dir = "./knowledge/ruliu"

    config = {
        "webhook_secret": webhook_secret,
        "target_name": target_name,
        "server_port": port,
        "output_dir": output_dir,
    }

    save_config(config)
    print(f"\n✅ 配置已保存到 {CONFIG_PATH}")
    print(f"\n下一步：")
    print(f"  python3 ruliu_auto_collector.py --start-server")
    print(f"  然后将回调 URL（http://你的IP:{port}/webhook）配置到如流机器人中")


# ─── Webhook 签名验证 ─────────────────────────────────────────────────────────

def verify_signature(secret: str, timestamp: str, nonce: str, body: bytes, signature: str) -> bool:
    """验证如流 Webhook 签名"""
    if not secret:
        return True  # 未配置签名密钥，跳过验证

    string_to_sign = f"{timestamp}\n{nonce}\n{body.decode('utf-8')}"
    hmac_code = hmac.new(
        secret.encode("utf-8"),
        string_to_sign.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    computed_signature = base64.b64encode(hmac_code).decode("utf-8")
    return hmac.compare_digest(computed_signature, signature)


# ─── Webhook Server ──────────────────────────────────────────────────────────

def create_app(config: dict) -> "Flask":
    """创建 Flask 应用，接收如流 Webhook 消息"""
    if not HAS_FLASK:
        print("错误：Webhook 服务需要 Flask：pip3 install flask", file=sys.stderr)
        sys.exit(1)

    app = Flask(__name__)
    output_dir = Path(config.get("output_dir", "./knowledge/ruliu"))
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl_path = output_dir / "messages_live.jsonl"
    secret = config.get("webhook_secret", "")

    @app.route("/webhook", methods=["POST"])
    def webhook():
        raw_body = flask_request.get_data()

        # 签名验证
        if secret:
            timestamp = flask_request.headers.get("X-Timestamp", "")
            nonce = flask_request.headers.get("X-Nonce", "")
            sig = flask_request.headers.get("X-Signature", "")
            if not verify_signature(secret, timestamp, nonce, raw_body, sig):
                return jsonify({"error": "签名验证失败"}), 403

        try:
            data = json.loads(raw_body)
        except json.JSONDecodeError:
            return jsonify({"error": "无效 JSON"}), 400

        # 如流 Webhook 消息体结构（适配常见格式）
        # 尝试多种字段名兼容
        msg_type = data.get("msgtype") or data.get("msg_type") or data.get("type", "text")

        # 提取发送人信息
        sender = (
            data.get("sender_name")
            or data.get("senderNick")
            or data.get("sender", {}).get("name", "")
            if isinstance(data.get("sender"), dict)
            else data.get("sender", "")
        )

        # 提取消息内容
        content = ""
        if msg_type == "text":
            text_obj = data.get("text", {})
            if isinstance(text_obj, dict):
                content = text_obj.get("content", "")
            elif isinstance(text_obj, str):
                content = text_obj
            if not content:
                content = data.get("content") or data.get("message", "")
        elif msg_type in ("markdown", "md"):
            md_obj = data.get("markdown", {})
            if isinstance(md_obj, dict):
                content = md_obj.get("text", "") or md_obj.get("content", "")
            elif isinstance(md_obj, str):
                content = md_obj
        elif msg_type == "rich_text":
            # 富文本消息，尝试提取纯文本
            rich = data.get("rich_text", {})
            if isinstance(rich, dict):
                paragraphs = rich.get("content", [])
                text_parts = []
                for para in paragraphs:
                    if isinstance(para, list):
                        for seg in para:
                            if isinstance(seg, dict) and seg.get("tag") in ("text", "a"):
                                text_parts.append(seg.get("text", ""))
                content = " ".join(text_parts)
        else:
            # 其他类型，尝试通用提取
            content = data.get("content") or data.get("text", "")
            if isinstance(content, dict):
                content = content.get("content", "") or content.get("text", "")

        if not content or not content.strip():
            return jsonify({"status": "skipped", "reason": "empty content"}), 200

        # 提取群信息
        chat_name = (
            data.get("chatName")
            or data.get("chat_name")
            or data.get("conversationTitle")
            or ""
        )

        # 构造统一记录
        record = {
            "sender": str(sender) if sender else "",
            "content": str(content).strip(),
            "chat_name": chat_name,
            "msg_type": msg_type,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "raw": data,
        }

        # 追加写入 JSONL
        with open(jsonl_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

        print(f"  [{record['timestamp']}][{chat_name}] {sender}: {content[:50]}...", file=sys.stderr)

        return jsonify({"status": "ok"}), 200

    @app.route("/health", methods=["GET"])
    def health():
        count = 0
        if jsonl_path.exists():
            count = sum(1 for _ in open(jsonl_path))
        return jsonify({"status": "running", "messages_count": count}), 200

    return app


def start_server(config: dict, port: Optional[int] = None) -> None:
    """启动 Webhook 接收服务"""
    if not HAS_FLASK:
        print("错误：Webhook 服务需要 Flask：pip3 install flask", file=sys.stderr)
        print("安装：pip3 install flask", file=sys.stderr)
        sys.exit(1)

    server_port = port or config.get("server_port", 8765)

    print(f"🚀 如流 Webhook 采集服务启动", file=sys.stderr)
    print(f"   端口：{server_port}", file=sys.stderr)
    print(f"   回调 URL：http://0.0.0.0:{server_port}/webhook", file=sys.stderr)
    print(f"   健康检查：http://0.0.0.0:{server_port}/health", file=sys.stderr)
    print(f"   消息存储：{config.get('output_dir', './knowledge/ruliu')}/messages_live.jsonl", file=sys.stderr)
    print(f"\n   将上述回调 URL 配置到如流群机器人的 Webhook 地址中", file=sys.stderr)
    print(f"   按 Ctrl+C 停止服务\n", file=sys.stderr)

    # 写入 PID 文件
    PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    PID_PATH.write_text(str(os.getpid()))

    app = create_app(config)

    try:
        app.run(host="0.0.0.0", port=server_port, debug=False)
    except KeyboardInterrupt:
        print("\n🛑 服务已停止", file=sys.stderr)
    finally:
        if PID_PATH.exists():
            PID_PATH.unlink()


def stop_server() -> None:
    """停止 Webhook 服务"""
    if not PID_PATH.exists():
        print("未找到运行中的服务", file=sys.stderr)
        return

    pid_str = PID_PATH.read_text().strip()
    try:
        pid = int(pid_str)
        os.kill(pid, signal.SIGTERM)
        print(f"✅ 已向进程 {pid} 发送停止信号", file=sys.stderr)
    except (ValueError, ProcessLookupError):
        print(f"进程 {pid_str} 不存在，清理 PID 文件", file=sys.stderr)
    finally:
        if PID_PATH.exists():
            PID_PATH.unlink()


# ─── 导出功能 ─────────────────────────────────────────────────────────────────

def export_messages(
    target_name: str,
    output_dir: Path,
    jsonl_path: Optional[Path] = None,
) -> str:
    """将 JSONL 消息记录导出为标准 messages.txt 格式"""
    output_dir.mkdir(parents=True, exist_ok=True)

    if jsonl_path is None:
        jsonl_path = output_dir / "messages_live.jsonl"

    if not jsonl_path.exists():
        return f"# 如流消息记录\n\n未找到消息文件：{jsonl_path}\n请先启动 Webhook 服务采集消息。\n"

    # 读取所有消息
    all_messages = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                all_messages.append(record)
            except json.JSONDecodeError:
                continue

    if not all_messages:
        return f"# 如流消息记录\n\n消息文件为空：{jsonl_path}\n"

    # 按目标人名过滤
    target_msgs = []
    other_msgs = []
    for msg in all_messages:
        sender = msg.get("sender", "")
        if target_name and target_name in sender:
            target_msgs.append(msg)
        else:
            other_msgs.append(msg)

    # 统计群来源
    chat_counts: dict[str, int] = {}
    for msg in target_msgs:
        chat = msg.get("chat_name", "未知群聊")
        chat_counts[chat] = chat_counts.get(chat, 0) + 1

    # 分类
    long_msgs = [m for m in target_msgs if len(m.get("content", "")) > 50]
    short_msgs = [m for m in target_msgs if len(m.get("content", "")) <= 50]

    # 格式化输出
    source_str = ", ".join(f"{k}（{v}条）" for k, v in chat_counts.items())
    lines = [
        f"# 如流消息记录（Webhook 采集）",
        f"目标：{target_name}",
        f"来源：{source_str}",
        f"共 {len(all_messages)} 条消息（目标用户 {len(target_msgs)} 条，其他 {len(other_msgs)} 条）",
        "",
        "---",
        "",
        "## 长消息（观点/决策/技术类）",
        "",
    ]

    for m in long_msgs:
        ts = m.get("timestamp", "")
        chat = m.get("chat_name", "")
        chat_str = f"[{chat}]" if chat else ""
        lines.append(f"[{ts}]{chat_str} {m['content']}")
        lines.append("")

    lines += ["---", "", "## 日常消息（风格参考）", ""]
    for m in short_msgs[:300]:
        ts = m.get("timestamp", "")
        lines.append(f"[{ts}] {m['content']}")

    # 对话上下文（包含其他人的消息，便于理解语境）
    if other_msgs:
        lines += ["", "---", "", "## 对话上下文（含其他人消息）", ""]
        # 按时间排序所有消息
        all_sorted = sorted(all_messages, key=lambda x: x.get("timestamp", ""))
        for m in all_sorted[:500]:
            sender = m.get("sender", "")
            ts = m.get("timestamp", "")
            is_target = target_name and target_name in sender
            who = f"[{target_name}]" if is_target else f"[{sender}]"
            lines.append(f"[{ts}] {who} {m['content']}")

    output = "\n".join(lines)

    # 写入文件
    msg_path = output_dir / "messages.txt"
    msg_path.write_text(output, encoding="utf-8")
    print(f"✅ 消息已导出到 {msg_path}（目标用户 {len(target_msgs)} 条）", file=sys.stderr)

    # 写入采集摘要
    summary = {
        "name": target_name,
        "source": "ruliu_webhook",
        "collected_at": datetime.now().isoformat(),
        "total_messages": len(all_messages),
        "target_messages": len(target_msgs),
        "chat_sources": chat_counts,
        "files": {"messages": str(msg_path)},
    }
    summary_path = output_dir / "collection_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    return output


# ─── 导出文件解析（路径 B）─────────────────────────────────────────────────────

def parse_export_file(
    file_path: str,
    target_name: str,
    output_dir: Path,
) -> str:
    """解析如流导出文件，复用 ruliu_parser 的逻辑"""
    # 尝试导入同目录下的 ruliu_parser
    script_dir = Path(__file__).parent
    sys.path.insert(0, str(script_dir))

    try:
        from ruliu_parser import parse_ruliu_json, parse_ruliu_txt, extract_key_content, format_output
    except ImportError:
        print("错误：找不到 ruliu_parser.py，请确保它与本脚本在同一目录", file=sys.stderr)
        sys.exit(1)

    fp = Path(file_path)
    if not fp.exists():
        print(f"错误：文件不存在 {fp}", file=sys.stderr)
        sys.exit(1)

    if fp.suffix.lower() == ".json":
        messages = parse_ruliu_json(str(fp), target_name)
    else:
        messages = parse_ruliu_txt(str(fp), target_name)

    if not messages:
        print(f"警告：未找到 '{target_name}' 发出的消息", file=sys.stderr)

    extracted = extract_key_content(messages)
    output = format_output(target_name, extracted)

    output_dir.mkdir(parents=True, exist_ok=True)
    msg_path = output_dir / "messages.txt"
    msg_path.write_text(output, encoding="utf-8")
    print(f"✅ 消息已解析并导出到 {msg_path}（共 {len(messages)} 条）", file=sys.stderr)

    # 写入采集摘要
    summary = {
        "name": target_name,
        "source": "ruliu_export_file",
        "source_file": str(fp),
        "collected_at": datetime.now().isoformat(),
        "total_messages": len(messages),
        "files": {"messages": str(msg_path)},
    }
    summary_path = output_dir / "collection_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))

    return output


# ─── 主流程 ───────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="如流数据自动采集器")
    parser.add_argument("--setup", action="store_true", help="初始化配置")
    parser.add_argument("--start-server", action="store_true", help="启动 Webhook 接收服务")
    parser.add_argument("--stop", action="store_true", help="停止 Webhook 服务")
    parser.add_argument("--port", type=int, default=None, help="Webhook 服务端口（默认 8765）")
    parser.add_argument("--export", action="store_true", help="将 JSONL 导出为标准 messages.txt")
    parser.add_argument("--target", default=None, help="目标用户的如流昵称")
    parser.add_argument("--output-dir", default=None, help="输出目录")
    parser.add_argument("--jsonl-path", default=None, help="JSONL 文件路径（默认 output-dir/messages_live.jsonl）")
    parser.add_argument("--parse-file", default=None, help="解析如流导出文件（路径 B）")

    args = parser.parse_args()

    if args.setup:
        setup_config()
        return

    if args.stop:
        stop_server()
        return

    if args.start_server:
        config = load_config()
        start_server(config, args.port)
        return

    if args.export:
        config = load_config()
        target = args.target or config.get("target_name", "")
        if not target:
            parser.error("请提供 --target（目标用户昵称）")

        output_dir = Path(args.output_dir) if args.output_dir else Path(config.get("output_dir", "./knowledge/ruliu"))
        jsonl_path = Path(args.jsonl_path) if args.jsonl_path else None

        export_messages(target, output_dir, jsonl_path)
        return

    if args.parse_file:
        config = load_config() if CONFIG_PATH.exists() else {}
        target = args.target or config.get("target_name", "")
        if not target:
            parser.error("请提供 --target（目标用户昵称）")

        output_dir = Path(args.output_dir) if args.output_dir else Path(config.get("output_dir", "./knowledge/ruliu"))
        parse_export_file(args.parse_file, target, output_dir)
        return

    parser.print_help()


if __name__ == "__main__":
    main()
