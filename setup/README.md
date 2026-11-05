# R1 环境安装脚本（Ubuntu Server 24.04 ARM64）

> 配套：[虚拟机准备](../docs/00-虚拟机准备.md)
> 版本（2026-10-25 已核对下载地址）：Kafka 4.3.2、Flink 1.20.5、HBase 2.6.7、Doris 4.0.8、MySQL 8.0

## 结论

在虚拟机里按顺序跑 7 个安装脚本（00–06），再跑一遍冒烟测试。冒烟测试三项全部通过，R1 环境就搭好了。

- **下载量**：约 4.8GB。其中 Doris 3.7GB、Flink 466MB、HBase 368MB、Kafka 129MB，Hadoop 客户端 jar 48MB。
- **耗时**：主要花在下载上，大约 1–2 小时。
- **重复运行**：脚本都可以放心重跑。已下载的包会跳过，配置每次都从备份重新生成。

---

## 一、文件说明

| 文件 | 作用 |
|---|---|
| `r1.env` | 统一配置：IP、密码、版本、目录、内存预算。**第一次只改前两节** |
| `00-base.sh` | 基础环境：时区、主机名解析、关 swap、内核参数、JDK 17、MySQL、Python |
| `01-mysql.sh` | MySQL：开 binlog（row 格式）、允许远程访问、建业务库 `r1_shop` 和账号 |
| `02-kafka.sh` | Kafka 4.3.2（KRaft 单节点，不需要 ZooKeeper） |
| `03-hbase.sh` | HBase 2.6.7（单机模式，自带 ZooKeeper） |
| `04-doris.sh` | Doris 4.0.8（FE + BE），并把 BE 注册到 FE |
| `05-flink.sh` | Flink 1.20.5，加上 Kafka、MySQL CDC、HBase、Doris 连接器，以及 MySQL 驱动和 Hadoop 客户端 |
| `06-python.sh` | Python 虚拟环境（回放器、画像模型用） |
| `bin/r1-start.sh`、`r1-stop.sh`、`r1-status.sh` | 日常启动、停止、看状态 |
| `smoke/run-smoke.sh` | 冒烟测试：三条链路都把数据写进 Doris |
| `conf/`、`lib/` | 配置模板和公共函数，一般不用改 |

## 二、安装步骤

**前提**：虚拟机已按《虚拟机准备》（docs/00-虚拟机准备.md）第二节建好，第三节第 1 步（固定 IP）已经做完。第三节的第 2–6 步由 `00-base.sh` 自动完成。

1. **把脚本复制到虚拟机**：在 Mac 终端执行（还没配 SSH 别名的话，把 `bigdata01` 换成 `用户名@虚拟机IP`）：
   ```bash
   rsync -a --exclude r1.env setup/ bigdata01:~/r1-setup/ && rsync -a --ignore-existing setup/r1.env bigdata01:~/r1-setup/   # 在仓库根目录执行；r1.env 只在第一次复制，之后不会覆盖虚拟机上改过的
   ```
2. **登录虚拟机，改配置**：执行 `ssh bigdata01`，再 `cd ~/r1-setup && nano r1.env`，改 `VM_IP`、`VM_SUBNET`、`MYSQL_APP_PASSWORD` 三项。
3. **按顺序运行**（每个脚本最后一行会提示下一步）：

   | 顺序 | 命令 | 成功的标志 |
   |---|---|---|
   | 1 | `bash 00-base.sh`，跑完**退出并重新登录** | 显示 swap 设备数 0、`vm.max_map_count` 为 2000000 |
   | 2 | `bash 01-mysql.sh` | `log_bin=ON`、`binlog_format=ROW` |
   | 3 | `bash 02-kafka.sh` | 测试 topic 能建能删 |
   | 4 | `bash 03-hbase.sh` | 打印出 `1 active master` |
   | 5 | `bash 04-doris.sh` | BE 已加入集群，查询结果为 ok |
   | 6 | `bash 05-flink.sh` | TaskManager 数 1，slot 总数 4 |
   | 7 | `bash 06-python.sh` | 依赖全部可用 |
   | 8 | `bash smoke/run-smoke.sh` | 三项检查都显示「通过」 |

4. **拍快照**：冒烟测试通过后，在 VMware 里拍一个快照，命名「R1 环境就绪」。
5. **省磁盘（可选）**：确认一切正常后，可以删掉 `/opt/software` 里的安装包，省出约 4.7GB。以后重跑安装脚本会重新下载。

## 三、日常使用

`00-base.sh` 已经把 `bin/` 加进了 PATH，重新登录后直接用：

| 场景 | 命令 |
|---|---|
| 开机后 | `r1-start.sh`（按依赖顺序启动全部组件，已在运行的会跳过） |
| 看状态 | `r1-status.sh`（进程、端口、各组件内存、虚拟机剩余内存和磁盘） |
| 关机前 | `r1-stop.sh`（MySQL 默认不停；要停就加参数 `mysql`） |
| 只启停某个组件 | 如 `r1-stop.sh flink`、`r1-start.sh flink` |

**网页**（在 Mac 浏览器打开）：
- Flink：`http://虚拟机IP:8081`
- Doris：`http://虚拟机IP:8030`（用户 root，密码为空）
- HBase：`http://虚拟机IP:16010`

## 四、设计说明（面试可以讲）

| 做法 | 原因 |
|---|---|
| Kafka 4.3.2，KRaft 模式 | Kafka 4.x 已彻底移除 ZooKeeper，元数据由内置的 Raft 控制器管理；单节点同时担任 broker 和 controller |
| Kafka 的 `transaction.max.timeout.ms` 调到 1 小时 | Flink 两阶段提交写 Kafka 时，事务要等 checkpoint 完成；broker 默认上限只有 15 分钟，会导致事务被提前中止 |
| Doris BE `mem_limit` 设为 4G，FE 堆设为 2G | 默认 BE 会用到物理内存的 90%、FE 堆 8G，和其他组件挤在一台 16GB 的机器上会互相抢内存 |
| 关 swap、调 `vm.max_map_count` 和文件句柄数 | Doris BE 启动时会检查这三项，不满足就拒绝启动 |
| HBase 单机模式，关闭 `hbase.unsafe.stream.capability.enforce` | 单机模式把 HMaster、RegionServer、ZooKeeper 放在一个进程里；本地文件系统不支持 hflush，必须关掉这项检查 |
| HBase 设 `hbase.zookeeper.property.clientPortAddress` 为 `0.0.0.0` | 单机模式内置的 ZooKeeper 默认只监听 127.0.0.1，而 quorum 写的是虚拟机主机名（解析到虚拟机 IP），会连接被拒、HMaster 退出。只监听虚拟机 IP 也不行，因为 HBase 的启动自检固定连 localhost。所以监听所有网卡（2026-11-02 实测发现） |
| MySQL binlog 用 row 格式 + full 行镜像 | Flink CDC 要拿到每一行变化前后的完整数据 |
| Flink 用 RocksDB 状态后端 + 增量 checkpoint | 和生产环境一致；大状态时只上传变化的部分 |
| Flink 的 lib 里放 hadoop-client-api / runtime | HBase 连接器运行时要用 Hadoop 的类（如 `org.apache.hadoop.io.Writable`）。用官方打包好、第三方依赖已重定位的客户端 jar，不用装整个 Hadoop，也不会和 Flink 的依赖冲突（2026-11-02 补充） |
| HBase 单机模式的 ZooKeeper 会话超时调到 90 秒（`zookeeper.session.timeout.localHBaseCluster`，`tickTime` 6 秒） | 默认只有 10 秒。2026-11-02 跑 MVP 时虚拟机整体卡了 23 秒（日志写「No GCs detected」），会话过期，HMaster 直接退出，下游的 DWD 作业跟着反复重启 |
| Doris FE 设 `routine_load_adaptive_min_batch_interval_sec = 10` | Doris 4 的「自适应批次」：Routine Load 有积压时每批强制至少 360 秒（换吞吐），实时回放期间数据要 6 分钟才可见、进度一格一格跳。练习环境要新鲜度，降到 10 秒后只落后约 1 分钟（2026-11-04 实测） |
| 给 HBase 连接器打补丁（`patches/zk-3.4-jdk14/`） | 连接器里打包的 ZooKeeper 3.4.14 客户端在 JDK 14+ 上解析不了主机名（ZOOKEEPER-3779），日志里是 `bigdata01/<unresolved>:2181` 反复重连，Lookup Join 报 ConnectionLoss。连接器没有更新版本，所以 `05-flink.sh` 编译改过的 `StaticHostProvider`（只改一行：用 `getHostString()` 取主机名）替换进 jar（2026-11-02 实测发现并修复） |
| 注释掉 `/etc/hosts` 里的 127.0.1.1 | 否则主机名会解析到回环地址，HBase、Kafka 只监听本机，从外面连不上 |
| 下载先走国内镜像，失败自动换官方地址 | 国内下载快；镜像缺版本时也不会卡住 |

## 五、已知风险与排错

| 问题 | 处理 |
|---|---|
| **冒烟测试第 3 项失败（HBase 连接器）** | 2026-11-02 已在 Flink 1.20.5 + JDK 17 上跑通（打补丁后）。**先排除 slot 不够**：冒烟测试要 3 个空闲 slot，R1 MVP 作业在跑、或在两个窗口同时跑了冒烟测试，HBase 作业都会拿不到 slot、结果为空（2026-11-04 实测；脚本现在会提前报错）。如果确实是连接器问题：报 ConnectionLoss、日志里有 `<unresolved>`，说明补丁没打上，重跑 `05-flink.sh`；报 `NoClassDefFoundError: org/apache/hadoop/...`，说明缺 Hadoop 客户端 jar，也是重跑 `05-flink.sh` |
| 浏览器打开网页报 503 | Mac 上的代理软件把虚拟机地址也转发了。在代理软件里把 `172.16.250.0/24` 加进不走代理的名单 |
| Doris BE 起不来 | 检查 swap、`vm.max_map_count`、`ulimit -n`（改完要重新登录才生效）；日志在 `/opt/module/doris/be/log/be.INFO` |
| Doris FE 起不来 | 日志在 `/opt/module/doris/fe/log/fe.log`；多半是 `priority_networks` 网段写错了 |
| Kafka 起不来 | 日志在 `/opt/module/kafka/logs/server.log`；如果改过 `node.id` 等关键配置，要清空 `/data/kafka/logs` 后重跑 `02-kafka.sh` |
| HBase 起不来 | 日志在 `/data/hbase/logs`；检查 `/etc/hosts` 里主机名是否解析到虚拟机 IP |
| Mac 打不开网页 | 先在虚拟机里跑 `r1-status.sh` 看组件和端口；再确认 Mac 能 ping 通虚拟机 IP |
| 下载慢或失败 | 脚本会自动换地址。也可以在 Mac 上下载好，`scp` 到虚拟机的 `/opt/software`，脚本会跳过下载 |
| 内存不够、虚拟机卡 | 用 `r1-status.sh` 看各组件内存；在 `r1.env` 调小预算后重跑对应的安装脚本 |
| Mac 磁盘紧张 | 虚拟机装完 R1 后，Mac 上约占 20–25GB。建议先清理 Docker Desktop 的数据（占 38GB），见《虚拟机准备》（docs/00-虚拟机准备.md）第七节 |
