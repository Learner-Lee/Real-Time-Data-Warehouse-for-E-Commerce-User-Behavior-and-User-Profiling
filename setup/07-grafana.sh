#!/usr/bin/env bash
# 第 7 步（可选）：Grafana 实时大屏，直连 Doris，每 5 秒刷新
# 数据源和大屏都用 provisioning 预置，装好打开 http://<虚拟机IP>:3000 就能看，不用在网页上配置
source "$(dirname "$0")/lib/common.sh"
check_env

GRAFANA_VERSION=${GRAFANA_VERSION:-12.3.0}
GRAFANA_HOME=${GRAFANA_HOME:-$MODULE_DIR/grafana}
GRAFANA_DATA=$DATA_DIR/grafana

PKG="grafana-$GRAFANA_VERSION.linux-arm64.tar.gz"
download "$PKG" "https://dl.grafana.com/oss/release/$PKG"
install_tarball "$PKG" "$GRAFANA_HOME"

mkdir -p "$GRAFANA_DATA/log" "$GRAFANA_DATA/plugins"

log "生成 conf/custom.ini（每次重新生成；Grafana 启动时会在 defaults.ini 之上读它）"
cat > "$GRAFANA_HOME/conf/custom.ini" <<EOF
[server]
http_port = 3000

[paths]
data = $GRAFANA_DATA
logs = $GRAFANA_DATA/log
plugins = $GRAFANA_DATA/plugins
provisioning = $GRAFANA_HOME/conf/provisioning

[analytics]
reporting_enabled = false
check_for_updates = false
check_for_plugin_updates = false

# 不登录也能看大屏（只读）；要改面板，用 admin 登录（默认 admin / admin，第一次登录会要求改密码）
[auth.anonymous]
enabled = true
org_role = Viewer

[dashboards]
default_home_dashboard_path = $GRAFANA_HOME/conf/r1-dashboards/r1-dashboard.json
min_refresh_interval = 5s
EOF

log "预置数据源和大屏（provisioning）"
rm -rf "$GRAFANA_HOME/conf/provisioning/datasources/r1-"* "$GRAFANA_HOME/conf/provisioning/dashboards/r1-"*
sed "s/@VM_HOST@/$VM_HOST/g" "$SCRIPT_DIR/conf/grafana/r1-datasource.yaml" > "$GRAFANA_HOME/conf/provisioning/datasources/r1-datasource.yaml"
sed "s#@DASHBOARD_DIR@#$GRAFANA_HOME/conf/r1-dashboards#g" "$SCRIPT_DIR/conf/grafana/r1-dashboards.yaml" > "$GRAFANA_HOME/conf/provisioning/dashboards/r1-dashboards.yaml"
mkdir -p "$GRAFANA_HOME/conf/r1-dashboards"
cp "$SCRIPT_DIR/conf/grafana/r1-dashboard.json" "$GRAFANA_HOME/conf/r1-dashboards/r1-dashboard.json"

# 已经在运行就重启一次，让新配置生效
if pgrep -f "$GRAFANA_HOME/bin/grafana server" >/dev/null || pgrep -f 'grafana server' >/dev/null; then
  log "Grafana 已在运行，重启以加载新配置"
  "$SCRIPT_DIR/bin/r1-stop.sh" grafana
fi
"$SCRIPT_DIR/bin/r1-start.sh" grafana

log "检查：健康状态"
curl -s "http://$VM_HOST:3000/api/health"
echo
log "完成。在 Mac 浏览器打开 http://$VM_IP:3000 （不登录就能看大屏）"
