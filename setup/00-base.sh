#!/usr/bin/env bash
# 第 0 步：基础环境。用普通用户运行，需要 sudo 的地方会提示输入密码。
# 固定 IP 要先按《虚拟机准备》（docs/00-虚拟机准备.md）第三节第 1 步手动配好，这个脚本不改网络。
source "$(dirname "$0")/lib/common.sh"
check_env
if [[ $EUID -eq 0 ]]; then
  die "请用普通用户运行，脚本需要时会自己调用 sudo"
fi

log "1/7 时区设为 Asia/Shanghai"
sudo timedatectl set-timezone Asia/Shanghai

log "2/7 主机名解析：注释掉 127.0.1.1 那一行，写入 $VM_IP $VM_HOST"
# Ubuntu 默认把主机名解析到 127.0.1.1，HBase、Kafka 会因此只监听本机回环地址，外面连不上
sudo sed -i -E 's/^(127\.0\.1\.1[[:space:]].*)$/# \1/' /etc/hosts
if ! awk -v ip="$VM_IP" -v host="$VM_HOST" '$1 == ip && $2 == host { found = 1 } END { exit !found }' /etc/hosts; then
  echo "$VM_IP $VM_HOST" | sudo tee -a /etc/hosts >/dev/null
fi
sudo hostnamectl set-hostname "$VM_HOST"

log "3/7 关闭 swap（Doris 要求）"
sudo swapoff -a
sudo sed -i -E 's@^([^#].*[[:space:]]swap[[:space:]].*)$@# \1@' /etc/fstab
sudo rm -f /swap.img

log "4/7 内核参数与文件句柄数（Doris 要求）"
echo "vm.max_map_count=2000000" | sudo tee /etc/sysctl.d/99-bigdata.conf >/dev/null
sudo sysctl --system >/dev/null
printf '* soft nofile 655350\n* hard nofile 655350\n' | sudo tee /etc/security/limits.d/99-bigdata.conf >/dev/null

log "5/7 安装基础软件：JDK 17、Python、MySQL 8.0、常用工具"
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  openjdk-17-jdk-headless python3-venv python3-pip python3-yaml \
  mysql-server mysql-client curl wget unzip jq net-tools htop

log "6/7 建目录，并交给当前用户"
sudo mkdir -p "$SOFT_DIR" "$MODULE_DIR" "$DATA_DIR"
sudo chown -R "$USER:$USER" "$SOFT_DIR" "$MODULE_DIR" "$DATA_DIR"
chmod +x "$SCRIPT_DIR"/*.sh "$SCRIPT_DIR"/bin/*.sh "$SCRIPT_DIR"/smoke/*.sh

log "7/7 写环境变量 ~/.r1_profile，并在 ~/.bashrc 里加载"
cat > "$HOME/.r1_profile" <<EOF
export JAVA_HOME=$JAVA_HOME
export KAFKA_HOME=$KAFKA_HOME
export FLINK_HOME=$FLINK_HOME
export HBASE_HOME=$HBASE_HOME
export DORIS_HOME=$DORIS_HOME
export PATH=\$JAVA_HOME/bin:\$KAFKA_HOME/bin:\$FLINK_HOME/bin:\$HBASE_HOME/bin:$SCRIPT_DIR/bin:\$PATH
EOF
if ! grep -q 'r1_profile' "$HOME/.bashrc"; then
  echo '[ -f ~/.r1_profile ] && source ~/.r1_profile' >> "$HOME/.bashrc"
fi

log "检查结果"
java -version 2>&1 | head -1
echo "swap 设备数：$(swapon --show | wc -l)（应为 0）"
echo "vm.max_map_count：$(sysctl -n vm.max_map_count)（应为 2000000）"
log "完成。请退出并重新登录（让文件句柄数和环境变量生效），然后运行 01-mysql.sh"
