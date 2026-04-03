#!/usr/bin/env python3
"""
如流消息导出文件解析器

支持的导出格式：
1. JSON 格式（数组或嵌套结构）
2. TXT 格式（每行：时间 发送人：内容）
3. 复制粘贴的纯文本

用法：
    python3 ruliu_parser.py --file messages.json --target "张三" --output output.txt
    python3 ruliu_parser.py --file messages.txt --target "张三" --output output.txt
"""

import json
import re
import sys
import argparse
from pathlib import Path
from datetime import datetime


def parse_ruliu_json(file_path: str, target_name: str) -> list[dict]:
    """解析如流消息导出的 JSON 格式"""
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = []

    # 兼容多种 JSON 结构
    if isinstance(data, list):
        raw_messages = data
    elif isinstance(data, dict):
        raw_messages = (
            data.get("messages")
            or data.get("records")
            or data.get("data")
            or data.get("msg_list")
            or data.get("chat_records")
            or []
        )
    else:
        return []

    for msg in raw_messages:
        # 兼容多种发送人字段名
        sender = (
            msg.get("sender_name")
            or msg.get("senderNick")
            or msg.get("sender")
            or msg.get("from")
            or msg.get("user_name")
            or msg.get("nick")
            or msg.get("from_name")
            or ""
        )
        if isinstance(sender, dict):
            sender = sender.get("name", "") or sender.get("nick", "")

        # 兼容多种内容字段名
        content = (
            msg.get("content")
            or msg.get("text")
            or msg.get("message")
            or msg.get("body")
            or msg.get("msg_body")
            or ""
        )

        # 兼容多种时间字段名
        timestamp = (
            msg.get("timestamp")
            or msg.get("create_time")
            or msg.get("time")
            or msg.get("send_time")
            or msg.get("msg_time")
            or ""
        )

        # content 可能是嵌套结构
        if isinstance(content, dict):
            content = content.get("text") or content.get("content") or str(content)
        if isinstance(content, list):
            content = " ".join(
                c.get("text", "") if isinstance(c, dict) else str(c)
                for c in content
            )

        # 时间戳处理
        if isinstance(timestamp, (int, float)):
            try:
                # 毫秒时间戳
                if timestamp > 1e12:
                    timestamp = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M")
                else:
                    timestamp = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
            except Exception:
                timestamp = str(timestamp)

        # 过滤：只保留目标人发送的消息
        if target_name and target_name not in str(sender):
            continue

        # 过滤：跳过系统消息、表情包、撤回消息
        content_str = str(content).strip()
        if not content_str or content_str in (
            "[图片]", "[文件]", "[撤回了一条消息]", "[语音]",
            "[表情]", "[名片]", "[位置]", "[视频]",
        ):
            continue

        messages.append({
            "sender": str(sender),
            "content": content_str,
            "timestamp": str(timestamp),
        })

    return messages


def parse_ruliu_txt(file_path: str, target_name: str) -> list[dict]:
    """解析如流消息导出的 TXT 格式（格式：时间 发送人：内容）"""
    messages = []

    with open(file_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    # 匹配多种常见格式：
    # 2024-01-01 10:00 张三：消息内容
    # 2024-01-01 10:00:00 张三: 消息内容
    # [2024-01-01 10:00] 张三：消息内容
    patterns = [
        # 标准格式：时间 发送人：内容
        re.compile(
            r"^(?P<time>\d{4}[-/]\d{1,2}[-/]\d{1,2}[\s\d:]*)\s+(?P<sender>.+?)[:：]\s*(?P<content>.+)$"
        ),
        # 方括号时间格式：[时间] 发送人：内容
        re.compile(
            r"^\[(?P<time>\d{4}[-/]\d{1,2}[-/]\d{1,2}[\s\d:]*)\]\s*(?P<sender>.+?)[:：]\s*(?P<content>.+)$"
        ),
        # 如流特有格式：发送人 时间
        re.compile(
            r"^(?P<sender>.+?)\s+(?P<time>\d{4}[-/]\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?)\s*$"
        ),
    ]

    current_sender = ""
    current_time = ""
    multiline_buffer = []

    def flush_buffer():
        nonlocal multiline_buffer
        if multiline_buffer and current_sender:
            content = "\n".join(multiline_buffer).strip()
            if content and (not target_name or target_name in current_sender):
                messages.append({
                    "sender": current_sender,
                    "content": content,
                    "timestamp": current_time,
                })
            multiline_buffer = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        matched = False
        for pattern in patterns[:2]:  # 只用前两个模式匹配消息行
            m = pattern.match(line)
            if m:
                flush_buffer()
                current_sender = m.group("sender").strip()
                current_time = m.group("time").strip()
                content = m.group("content").strip()

                # 跳过系统消息
                if content in ("[图片]", "[文件]", "[撤回了一条消息]", "[语音]", "[表情]"):
                    current_sender = ""
                    matched = True
                    break

                multiline_buffer = [content]
                matched = True
                break

        if not matched:
            # 可能是多行消息的续行
            if current_sender:
                multiline_buffer.append(line)
            else:
                # 无格式纯文本，检查是否包含目标人名
                if target_name and target_name in line:
                    messages.append({
                        "sender": target_name,
                        "content": line,
                        "timestamp": "",
                    })

    flush_buffer()
    return messages


def extract_key_content(messages: list[dict]) -> dict:
    """
    对消息进行分类提取，区分：
    - 长消息（>50字）：可能包含观点、方案、技术判断
    - 决策类回复：包含关键决策词
    - 日常沟通：其他消息
    """
    long_messages = []
    decision_messages = []
    daily_messages = []

    decision_keywords = [
        "同意", "不行", "觉得", "建议", "应该", "不应该", "可以", "不可以",
        "方案", "思路", "考虑", "决定", "确认", "拒绝", "推进", "暂缓",
        "没问题", "有问题", "风险", "评估", "判断",
        "拉个小群", "先看看", "排期", "上线", "回滚", "hotfix",
    ]

    for msg in messages:
        content = msg["content"]

        if len(content) > 50:
            long_messages.append(msg)
        elif any(kw in content for kw in decision_keywords):
            decision_messages.append(msg)
        else:
            daily_messages.append(msg)

    return {
        "long_messages": long_messages,
        "decision_messages": decision_messages,
        "daily_messages": daily_messages,
        "total_count": len(messages),
    }


def format_output(target_name: str, extracted: dict) -> str:
    """格式化输出，供 AI 分析使用"""
    lines = [
        f"# 如流消息提取结果",
        f"目标人物：{target_name}",
        f"总消息数：{extracted['total_count']}",
        "",
        "---",
        "",
        "## 长消息（观点/方案类，权重最高）",
        "",
    ]

    for msg in extracted["long_messages"]:
        ts = f"[{msg['timestamp']}] " if msg["timestamp"] else ""
        lines.append(f"{ts}{msg['content']}")
        lines.append("")

    lines += [
        "---",
        "",
        "## 决策类回复",
        "",
    ]

    for msg in extracted["decision_messages"]:
        ts = f"[{msg['timestamp']}] " if msg["timestamp"] else ""
        lines.append(f"{ts}{msg['content']}")
        lines.append("")

    lines += [
        "---",
        "",
        "## 日常沟通（风格参考）",
        "",
    ]

    # 日常消息只取前 100 条，避免太长
    for msg in extracted["daily_messages"][:100]:
        ts = f"[{msg['timestamp']}] " if msg["timestamp"] else ""
        lines.append(f"{ts}{msg['content']}")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="解析如流消息导出文件")
    parser.add_argument("--file", required=True, help="输入文件路径（.json 或 .txt）")
    parser.add_argument("--target", required=True, help="目标人物姓名（只提取此人发出的消息）")
    parser.add_argument("--output", default=None, help="输出文件路径（默认打印到 stdout）")

    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"错误：文件不存在 {file_path}", file=sys.stderr)
        sys.exit(1)

    # 根据文件类型选择解析器
    if file_path.suffix.lower() == ".json":
        messages = parse_ruliu_json(str(file_path), args.target)
    else:
        messages = parse_ruliu_txt(str(file_path), args.target)

    if not messages:
        print(f"警告：未找到 '{args.target}' 发出的消息", file=sys.stderr)
        print("提示：请检查目标姓名是否与文件中的发送人名称一致", file=sys.stderr)

    extracted = extract_key_content(messages)
    output = format_output(args.target, extracted)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"已输出到 {args.output}，共 {len(messages)} 条消息")
    else:
        print(output)


if __name__ == "__main__":
    main()
