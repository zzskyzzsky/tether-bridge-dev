#!/usr/bin/env python3
"""出站噪音判定回归：_is_noise_output 的成对断言。

规则（不可退化）：精确匹配 + 两端包裹符归一化，绝不做裸子串。
反例必须包含历史误杀用例（「检查完成，日志无输出异常」「NO_REPLY 机制我已理解…」）
以及 single-char 表项（「空」）最容易带出的「空指针异常」类正文。

用法：python3 tests/test_watcher_noise.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import tether_watcher as tw  # noqa: E402

NOISE = [
    "（空）", "(空)", "空", "（空）。",
    "（不产生任何输出）", "NO_REPLY。", "确认，standby。", "已确认",
]
REAL = [
    "结论如下：DB 已迁移",
    "检查完成，日志无输出异常",
    "空指针异常：tether_watcher.py:1393 抛 None",
    "（空）不是占位符吗？这是实质提问",
    "NO_REPLY 机制我已理解，结论如下",
]


def main():
    fails = []
    for text in NOISE:
        if not tw._is_noise_output(text):
            fails.append(("应判噪音却放行", text))
    for text in REAL:
        if tw._is_noise_output(text):
            fails.append(("应放行却判噪音", text))
    total = len(NOISE) + len(REAL)
    if fails:
        for kind, text in fails:
            print(f"FAIL {kind}: {text!r}")
        print(f"\n{len(fails)}/{total} 失败")
        return 1
    print(f"PASS {total}/{total}（噪音 {len(NOISE)} + 放行 {len(REAL)}）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
