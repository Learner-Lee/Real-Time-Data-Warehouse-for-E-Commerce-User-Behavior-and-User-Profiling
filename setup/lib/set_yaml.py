"""把 a.b.c=value 形式的配置写进 Flink 的 config.yaml（嵌套 YAML），文件里其他配置原样保留。

用法：python3 set_yaml.py config.yaml jobmanager.rpc.address=bigdata01 taskmanager.numberOfTaskSlots=4
"""
import sys

import yaml


def convert(value):
    """把字符串转成 YAML 里合适的类型：true/false 转布尔，纯数字转整数，其余保持字符串。"""
    if value in ("true", "false"):
        return value == "true"
    try:
        return int(value)
    except ValueError:
        return value


def set_value(conf, dotted_key, value):
    node = conf
    parts = dotted_key.split(".")
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[parts[-1]] = convert(value)


def main(path, assignments):
    with open(path, encoding="utf-8") as f:
        conf = yaml.safe_load(f) or {}
    for item in assignments:
        key, value = item.split("=", 1)
        set_value(conf, key, value)
    with open(path, "w", encoding="utf-8") as f:
        # width 设得很大，避免 env.java.opts.all 这种长参数被折成多行
        yaml.safe_dump(conf, f, sort_keys=False, allow_unicode=True, width=100000)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2:])
