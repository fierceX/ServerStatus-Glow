<p align="center">
    <h1 align="center">✨ ServerStatus-Glow</h1>
</p>


## 1. 介绍
  基于 [ServerStatus-Rust](https://github.com/zdz/ServerStatus-Rust) 版本进行了修改，主要增加了以下功能：
  - 完善对FreeBSD系统的支持，包括`进程数`、`线程数`、`TCP`、`UDP`等。
  - 修改磁盘监控的方式，详细列出磁盘及占用情况。
  - 修复`zfs`文件系统的统计错误的问题，采用`zpool list`的结果进行统计。

## 2. 主题
  主题为 [ServerStatus-Theme-Glow](https://github.com/fierceX/ServerStatus-Theme-Glow)，基于 [ServerStatus-Theme-Light](https://github.com/orilights/ServerStatus-Theme-Light) 的主题进行了修改，适配上述修改的磁盘监控展示方式，并添加了了内存和网络历史图表。
## 3. 安装
  将修改后的主题进行编译
  ```
  pnpm install
  pnpm build
  ```
  将编译好的dist目录下的文件复制到web目录下（原web目录下的jinja文件夹不要删除）
  然后和原项目一样进行编译。
  ```
  cargo build --release
  ```
## 感谢

- [zdz/ServerStatus-Rust](https://github.com/zdz/ServerStatus-Rust)
- [orilights/ServerStatus-Theme-Light](https://github.com/orilights/ServerStatus-Theme-Light)
