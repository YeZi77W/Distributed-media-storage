# Distributed Media Storage

基于 C++17 的 Linux 分布式媒体文件存储项目，提供文件上传、寻址、下载和删除，以及断点续传。Tracker 控制面使用自研 HighPerfRPC；文件数据通过 Storage 服务传输。项目适合作为媒体文件的数据入口，不包含转码、切片或完整流媒体分发功能。

## 架构与能力

- Tracker：维护节点状态，将用户路由到存储组；MySQL 保存路由，Redis 提供缓存。
- ID Server：通过 MySQL 事务分配路径号段，支持多个实例。
- Storage：同组节点共享挂载卷和元数据库，支持故障切换、上传会话、文件发布与删除恢复。
- Client：支持分块上传、断点续传、范围读取、下载校验及删除。
- `src/06_http` 含 HTTP Range 实现；当前 CMake 构建与演示未接入 HTTP 目标。

同组共享卷是共同依赖，项目不负责副本复制；示例演示为单机多进程，并非多机容灾证明。配置、数据库、协议和部分程序保留历史 `tnv` 标识，以兼容现有实现。

## 依赖与构建

需要 Linux、CMake 3.20+、C++17 编译器、ACL C/C++ (`libacl_all`)、MySQL/MariaDB 客户端开发库、OpenSSL、匹配版本的 Protobuf 编译器与完整开发库。运行演示还需 MySQL、Redis、Python 3 和 `mountpoint`。HighPerfRPC 源码包含在 `third_party/HighPerfRPC` 中。

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DACL_ROOT=/path/to/acl
cmake --build build -j4
```

ACL 已安装到系统路径时可省略 `ACL_ROOT`；也可指定 `ACL_CPP_INCLUDE_DIR`、`ACL_INCLUDE_DIR` 和 `ACL_LIBRARY`。构建生成 `build/bin/{tracker,id,storage,client,tnv_bench,tnv_file_bench}`。需要替换 RPC 源码时可设置 `HIGHPERFRPC_SOURCE_DIR`。

## 演示

以下使用隔离的测试环境。先准备 MySQL、Redis 和一个可写的共享挂载点；示例脚本要求 `/mnt/tnv/group001` 已挂载。复制 `etc/demo/*.cfg` 到项目外，填写测试数据库账号及密码。`sql/demo.sql` 创建三个演示数据库，不覆盖已有库；需事先创建账号并授予对应权限。`sql/id.sql`、`tracker.sql` 和 `storage.sql` 含 DROP DATABASE，执行前务必确认目标。

```bash
export PROJECT="$PWD"
export BUILD="$PROJECT/build"
mkdir -p "$HOME/media-demo/config" "$HOME/media-demo/run"
cp "$PROJECT"/etc/demo/*.cfg "$HOME/media-demo/config/"
# 编辑配置中的数据库凭据，然后在隔离环境中初始化
mysql -u root -p < "$PROJECT/sql/demo.sql"
# 按实际环境准备并挂载 /mnt/tnv/group001
bash "$PROJECT/scripts/start_demo.sh" --build "$BUILD" --config-dir "$HOME/media-demo/config" --run-dir "$HOME/media-demo/run"
python3 "$PROJECT/scripts/smoke_client.py" --client "$BUILD/bin/client"
bash "$PROJECT/scripts/stop_demo.sh" --run-dir "$HOME/media-demo/run"
```

演示服务使用 Tracker 21010、ID 22000、Storage 23000/23001；日志位于 run 目录。客户端用法可通过不带参数运行 `build/bin/client` 查看。测试环境中的凭据、数据库和业务文件不要提交至 GitHub。

## 目录与测试

`src/` 为服务和客户端，`proto/` 为控制面协议，`etc/` 为示例配置，`sql/` 为建库及迁移脚本，`scripts/` 为演示与采集工具，`tests/` 为验收脚本和说明，`third_party/HighPerfRPC/` 为构建所需 RPC 框架源码。参见 [测试说明](tests/README.md) 及各测试文档。Python 聚合单元测试可通过 `python3 -m unittest discover -s tests -p 'test_*.py'` 运行；其余集成测试需要已启动的 Linux 服务。
