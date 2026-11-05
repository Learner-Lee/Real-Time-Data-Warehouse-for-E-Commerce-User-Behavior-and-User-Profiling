#!/usr/bin/env bash
# 各脚本共用的函数：读取 r1.env、打日志、下载、解压、等端口

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=../r1.env
source "$SCRIPT_DIR/r1.env"
export JAVA_HOME

log()  { printf '\033[32m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$*"; }
warn() { printf '\033[33m[警告] %s\033[0m\n' "$*"; }
die()  { printf '\033[31m[错误] %s\033[0m\n' "$*" >&2; exit 1; }

# 确认 r1.env 已经改过，并且是在 ARM64 的 Linux 上运行
check_env() {
  if [[ "$VM_IP" == *x* || "$VM_SUBNET" == *x* ]]; then
    die "先在 r1.env 里把 VM_IP 和 VM_SUBNET 改成你虚拟机的实际地址"
  fi
  if [[ "$(uname -s)-$(uname -m)" != "Linux-aarch64" ]]; then
    die "这套脚本是给 ARM64 的 Ubuntu 虚拟机写的，当前是 $(uname -s)-$(uname -m)"
  fi
}

check_password() {
  if [[ "$MYSQL_APP_PASSWORD" == "CHANGE_ME" ]]; then
    die "先在 r1.env 里把 MYSQL_APP_PASSWORD 改成你自己的密码"
  fi
  # 密码会被写进 SQL 和配置文件，这几个字符会破坏语法
  case "$MYSQL_APP_PASSWORD" in
    *[\'\"\ \|\&\\]*) die "MYSQL_APP_PASSWORD 里不要用引号、空格和 | & \\ 这几个字符" ;;
  esac
}

# 依次尝试多个地址，下载到 $SOFT_DIR；文件已经存在就跳过
download() {   # 用法：download 文件名 地址1 [地址2 ...]
  local name=$1 url
  shift
  local target="$SOFT_DIR/$name"
  if [[ -s "$target" ]]; then
    log "已下载过，跳过：$name"
    return
  fi
  for url in "$@"; do
    log "下载 $url"
    if curl -fL --retry 3 --connect-timeout 15 -o "$target.part" "$url"; then
      mv "$target.part" "$target"
      return
    fi
    warn "这个地址失败了，换下一个"
  done
  rm -f "$target.part"
  die "所有地址都下载失败：$name"
}

# 把 Apache 项目的相对路径拼到每个镜像上，例如 apache_urls kafka/4.3.2/kafka_2.13-4.3.2.tgz
apache_urls() {
  local m
  for m in $APACHE_MIRRORS; do
    echo "$m/$1"
  done
}

# 拼出 Maven 仓库里 jar 包的地址，例如 maven_urls org.apache.flink flink-sql-connector-kafka 3.4.0-1.20
maven_urls() {
  local group_path=${1//.//} artifact=$2 version=$3 m
  for m in $MAVEN_MIRRORS; do
    echo "$m/$group_path/$artifact/$version/$artifact-$version.jar"
  done
}

# 解压到 $MODULE_DIR，再建一个不带版本号的软链接（如 /opt/module/kafka），以后升级只改软链接
install_tarball() {   # 用法：install_tarball 安装包文件名 软链接路径
  local tarball=$1 link=$2 top
  # awk 读完整个列表再退出，避免 tar 因管道提前关闭而报错
  top=$(tar -tzf "$SOFT_DIR/$tarball" | awk -F/ 'NR == 1 { print $1 }')
  if [[ ! -d "$MODULE_DIR/$top" ]]; then
    log "解压 $tarball（大包要等一会儿）"
    tar -xzf "$SOFT_DIR/$tarball" -C "$MODULE_DIR"
  fi
  ln -sfn "$MODULE_DIR/$top" "$link"
  log "已安装：$link -> $MODULE_DIR/$top"
}

# 等某个端口能连上，超时就报错
wait_port() {   # 用法：wait_port 端口 描述 [超时秒数]
  local port=$1 desc=$2 timeout=${3:-60} i
  for ((i = 0; i < timeout; i++)); do
    if (exec 3<>"/dev/tcp/$VM_HOST/$port") 2>/dev/null; then
      log "$desc 已就绪（端口 $port）"
      return 0
    fi
    sleep 1
  done
  die "$desc 在 ${timeout} 秒内没有起来，先看它的日志"
}

# 在 Doris 里执行一条 SQL（root 默认没有密码），输出不带表头
doris_sql() {
  mysql -h "$VM_HOST" -P 9030 -uroot -N -e "$1"
}
