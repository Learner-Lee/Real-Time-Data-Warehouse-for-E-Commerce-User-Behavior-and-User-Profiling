#!/usr/bin/env bash
# 第 1 步：MySQL 8.0（第 0 步已用 apt 装好）。开 binlog（row 格式）、允许远程访问、建业务库和账号
source "$(dirname "$0")/lib/common.sh"
check_env
check_password

log "写配置 /etc/mysql/mysql.conf.d/r1.cnf（文件名排在 mysqld.cnf 之后，会覆盖默认值）"
sudo tee /etc/mysql/mysql.conf.d/r1.cnf >/dev/null <<EOF
[mysqld]
# 允许虚拟机外（比如 Mac）连接
bind-address               = 0.0.0.0
# Flink CDC 读 binlog 的前提：开 binlog、row 格式、记录完整的行
server-id                  = 1
log_bin                    = mysql-bin
binlog_format              = ROW
binlog_row_image           = FULL
binlog_expire_logs_seconds = 604800
default-time-zone          = '+08:00'
character-set-server       = utf8mb4
innodb_buffer_pool_size    = $MYSQL_BUFFER_POOL
EOF
sudo systemctl restart mysql
wait_port 3306 "MySQL"

log "建业务库 r1_shop 和账号 $MYSQL_APP_USER（带 Flink CDC 需要的复制权限）"
sudo mysql <<EOF
CREATE DATABASE IF NOT EXISTS r1_shop DEFAULT CHARACTER SET utf8mb4;
CREATE USER IF NOT EXISTS '$MYSQL_APP_USER'@'%' IDENTIFIED BY '$MYSQL_APP_PASSWORD';
ALTER USER '$MYSQL_APP_USER'@'%' IDENTIFIED BY '$MYSQL_APP_PASSWORD';
GRANT ALL PRIVILEGES ON r1_shop.* TO '$MYSQL_APP_USER'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '$MYSQL_APP_USER'@'%';
FLUSH PRIVILEGES;
EOF

log "检查（应为 log_bin=ON、binlog_format=ROW、binlog_row_image=FULL、bind_address=0.0.0.0）"
sudo mysql -e "SHOW VARIABLES WHERE Variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image', 'bind_address', 'time_zone');"
log "完成。下一步运行 02-kafka.sh"
