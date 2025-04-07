use anyhow::Result;
use chrono::{Utc};
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use crate::payload::HostStat;

pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    pub fn new(db_path: &str) -> Result<Self> {
        let path = Path::new(db_path);
        let need_init = !path.exists();

        let conn = Connection::open(db_path)?;

        // 开启 WAL 模式和其他性能优化
        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = 1000;
            PRAGMA temp_store = MEMORY;
            PRAGMA mmap_size = 30000000000;
        ")?;

        if need_init {
            Self::init_db(&conn)?;
        }

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    // 在 Database 结构体的实现中添加以下方法

    // 更新主机的last_network数据
    pub fn update_last_network(&self, host_name: &str, network_in: u64, network_out: u64) -> Result<()> {
        let conn = self.conn.lock().unwrap();

        // 首先获取主机ID
        let mut stmt = conn.prepare("SELECT id FROM hosts WHERE name = ?")?;
        let host_id: Option<i64> = stmt.query_row(params![host_name], |row| row.get(0)).ok();

        if let Some(id) = host_id {
            // 检查是否已有last_network记录
            let mut check_stmt = conn.prepare("SELECT COUNT(*) FROM last_network WHERE host_id = ?")?;
            let count: i64 = check_stmt.query_row(params![id], |row| row.get(0))?;

            if count > 0 {
                // 更新现有记录
                conn.execute(
                    "UPDATE last_network SET network_in = ?, network_out = ?, updated_at = ? WHERE host_id = ?",
                    params![network_in as i64, network_out as i64, Utc::now().timestamp(), id],
                )?;
            } else {
                // 创建新记录
                conn.execute(
                    "INSERT INTO last_network (host_id, network_in, network_out, updated_at) VALUES (?, ?, ?, ?)",
                    params![id, network_in as i64, network_out as i64, Utc::now().timestamp()],
                )?;
            }

            Ok(())
        } else {
            Err(anyhow::anyhow!("Host not found: {}", host_name))
        }
    }

    // 获取所有主机的last_network数据
    pub fn get_last_network_data(&self) -> Result<Vec<(String, u64, u64)>> {
        let conn = self.conn.lock().unwrap();
        let mut result = Vec::new();

        let mut stmt = conn.prepare(
            "SELECT h.name, ln.network_in, ln.network_out
             FROM last_network ln
             JOIN hosts h ON ln.host_id = h.id"
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
            ))
        })?;

        for row_result in rows {
            result.push(row_result?);
        }

        Ok(result)
    }

    // 在init_db方法中添加last_network表的创建
    fn init_db(conn: &Connection) -> Result<()> {
        // 主机表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS hosts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                alias TEXT,
                UNIQUE(name)
            )",
            [],
        )?;

        // 简化的统计数据表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats (
                id INTEGER PRIMARY KEY,
                host_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                cpu_usage REAL,
                memory_total INTEGER,
                memory_used INTEGER,
                network_in INTEGER,
                network_out INTEGER,
                network_in_speed INTEGER,
                network_out_speed INTEGER,
                online BOOLEAN,
                FOREIGN KEY (host_id) REFERENCES hosts(id)
            )",
            [],
        )?;

        // 磁盘数据表 - 每个磁盘单独记录
        conn.execute(
            "CREATE TABLE IF NOT EXISTS disk_stats (
                id INTEGER PRIMARY KEY,
                host_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                mount_point TEXT NOT NULL,
                disk_total INTEGER,
                disk_used INTEGER,
                FOREIGN KEY (host_id) REFERENCES hosts(id)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregated_stats (
                id INTEGER PRIMARY KEY,
                host_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                interval_minutes INTEGER NOT NULL,
                cpu_usage REAL,
                memory_total INTEGER,
                memory_used INTEGER,
                network_in INTEGER,
                network_out INTEGER,
                network_in_speed INTEGER,
                network_out_speed INTEGER,
                online BOOLEAN,
                FOREIGN KEY (host_id) REFERENCES hosts(id),
                UNIQUE(host_id, timestamp, interval_minutes)
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregated_disk_stats (
                id INTEGER PRIMARY KEY,
                host_id INTEGER NOT NULL,
                timestamp INTEGER NOT NULL,
                interval_minutes INTEGER NOT NULL,
                mount_point TEXT NOT NULL,
                disk_total INTEGER,
                disk_used INTEGER,
                FOREIGN KEY (host_id) REFERENCES hosts(id),
                UNIQUE(host_id, timestamp, interval_minutes, mount_point)
            )",
            [],
        )?;

        // ... existing code ...

        // 为聚合表添加索引
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agg_stats_host_time ON aggregated_stats(host_id, timestamp, interval_minutes)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agg_disk_stats_host_time ON aggregated_disk_stats(host_id, timestamp, interval_minutes)",
            [],
        )?;

        // 优化索引 - 添加更多索引以加速查询
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_host_time ON stats(host_id, timestamp)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_disk_stats_host_time ON disk_stats(host_id, timestamp)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_timestamp ON stats(timestamp)",
            [],
        )?;

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_disk_stats_timestamp ON disk_stats(timestamp)",
            [],
        )?;

        // 添加last_network表
        conn.execute(
            "CREATE TABLE IF NOT EXISTS last_network (
                id INTEGER PRIMARY KEY,
                host_id INTEGER NOT NULL,
                network_in INTEGER NOT NULL,
                network_out INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY (host_id) REFERENCES hosts(id),
                UNIQUE(host_id)
            )",
            [],
        )?;

        Ok(())
    }

    // 在 save_stat 方法中
    // 修复 save_stat 方法中的事务处理
    pub fn save_stat(&self, stat: &HostStat) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();

        // 确保主机存在
        let host_id = self.ensure_host_exists(&conn, stat)?;

        // 开始事务
        let tx = conn.transaction()?;

        // 保存简化的统计数据
        tx.execute(
            "INSERT INTO stats (
                host_id, timestamp, cpu_usage, memory_total, memory_used,
                network_in, network_out, network_in_speed, network_out_speed, online
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params![
                host_id,
                stat.latest_ts,
                stat.cpu,
                stat.memory_total,
                stat.memory_used,
                stat.network_in,
                stat.network_out,
                stat.network_rx,
                stat.network_tx,
                stat.online4 || stat.online6
            ],
        )?;

        // 保存每个磁盘的数据 - 使用预处理语句
        if !stat.disks.is_empty() {
            let mut disk_stmt = tx.prepare(
                "INSERT INTO disk_stats (
                    host_id, timestamp, mount_point, disk_total, disk_used
                ) VALUES (?, ?, ?, ?, ?)"
            )?;

            for disk in &stat.disks {
                disk_stmt.execute(params![
                    host_id,
                    stat.latest_ts,
                    disk.mount_point,
                    disk.total,
                    disk.used
                ])?;
            }
        }

        // 提交事务
        tx.commit()?;

        Ok(())
    }

    fn ensure_host_exists(&self, conn: &Connection, stat: &HostStat) -> Result<i64> {
        let mut stmt = conn.prepare("SELECT id FROM hosts WHERE name = ?")?;
        let host_id: Option<i64> = stmt.query_row(params![stat.name], |row| row.get(0)).ok();

        if let Some(id) = host_id {
            // 更新别名
            if !stat.alias.is_empty() {
                conn.execute(
                    "UPDATE hosts SET alias = ? WHERE id = ?",
                    params![stat.alias, id],
                )?;
            }
            Ok(id)
        } else {
            conn.execute(
                "INSERT INTO hosts (name, alias) VALUES (?, ?)",
                params![stat.name, stat.alias],
            )?;
            Ok(conn.last_insert_rowid())
        }
    }

    // 在 Database 实现中添加
    pub fn get_stats_by_timerange(&self, start_time: i64, end_time: i64) -> Result<HashMap<String, Vec<HostStatRecord>>> {
        let conn = self.conn.lock().unwrap();
        let mut result = HashMap::new();

        // 计算时间范围的长度（秒）
        let time_range = end_time - start_time;

        // 根据时间范围选择合适的聚合级别
        // 超过3天使用1小时聚合，超过1天使用30分钟聚合，超过12小时使用15分钟聚合，超过6小时使用5分钟聚合
        let interval_minutes = if time_range > 3 * 24 * 3600 {
            60 // 1小时
        } else if time_range >= 24 * 3600 {
            30 // 30分钟
        } else if time_range >= 12 * 3600 {
            15 // 15分钟
        } else if time_range >= 1 * 3600 {
            5  // 5分钟
        } else {
            0  // 使用原始数据
        };

        // 获取时间范围内的所有主机
        let mut hosts_stmt = conn.prepare(
            "SELECT DISTINCT h.id, h.name, h.alias
             FROM hosts h
             JOIN stats s ON h.id = s.host_id
             WHERE s.timestamp BETWEEN ? AND ?"
        )?;

        let hosts = hosts_stmt.query_map(params![start_time, end_time], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2).unwrap_or_default(),
            ))
        })?;


        // 最大数据点数量，默认600
    let max_points = 600;

    for host_result in hosts {
        let (host_id, host_name, host_alias) = host_result?;

        // 如果使用聚合数据且聚合级别大于0
        if interval_minutes > 0 {
            // 检查聚合表中是否有足够的数据
            let mut count_agg_stmt = conn.prepare(
                "SELECT COUNT(*) FROM aggregated_stats
                 WHERE host_id = ? AND timestamp BETWEEN ? AND ? AND interval_minutes = ?"
            )?;

            let agg_count: i64 = count_agg_stmt.query_row(
                params![host_id, start_time, end_time, interval_minutes],
                |row| row.get(0)
            )?;

            // 如果聚合表中有足够的数据
            if agg_count > 0 {
                // 使用聚合表查询
                let mut stats_stmt = conn.prepare(
                    "SELECT timestamp, cpu_usage, memory_total, memory_used,
                            network_in, network_out, network_in_speed, network_out_speed, online
                     FROM aggregated_stats
                     WHERE host_id = ? AND timestamp BETWEEN ? AND ? AND interval_minutes = ?
                     ORDER BY timestamp ASC
                     LIMIT ?"
                )?;

                let stats = stats_stmt.query_map(
                    params![host_id, start_time, end_time, interval_minutes, max_points],
                    |row| {
                        Ok(HostStatRecord {
                            timestamp: row.get(0)?,
                            cpu: row.get(1)?,
                            memory_total: row.get::<_, f64>(2)? as i64,
                            memory_used: row.get::<_, f64>(3)? as i64,
                            network_in: row.get::<_, f64>(4)? as i64,
                            network_out: row.get::<_, f64>(5)? as i64,
                            network_in_speed: row.get::<_, f64>(6)? as i64,
                            network_out_speed: row.get::<_, f64>(7)? as i64,
                            online: row.get(8)?,
                            alias: host_alias.clone(),
                            disks: Vec::new(),
                        })
                    }
                )?;

                let mut host_stats = Vec::new();
                for stat_result in stats {
                    host_stats.push(stat_result?);
                }

                // 如果有统计数据，获取聚合的磁盘数据
                if !host_stats.is_empty() {
                    for stat in &mut host_stats {
                        // 从聚合磁盘表中获取对应时间戳的磁盘数据
                        let mut disks_stmt = conn.prepare(
                            "SELECT timestamp, mount_point, disk_total, disk_used
                             FROM aggregated_disk_stats
                             WHERE host_id = ? AND timestamp = ? AND interval_minutes = ?"
                        )?;

                        let disks = disks_stmt.query_map(
                            params![host_id, stat.timestamp, interval_minutes],
                            |row| {
                                Ok(DiskRecord {
                                    timestamp: row.get(0)?,
                                    mount_point: row.get(1)?,
                                    total: row.get::<_, f64>(2)? as i64,
                                    used: row.get::<_, f64>(3)? as i64,
                                })
                            }
                        )?;

                        for disk_result in disks {
                            stat.disks.push(disk_result?);
                        }
                    }
                }

                result.insert(host_name, host_stats);
                continue; // 已经处理完这个主机，继续下一个
            }
        }else{
                // 使用原始查询 - 添加LIMIT以防止返回过多数据
                let mut stats_stmt = conn.prepare(
                    "SELECT timestamp, cpu_usage, memory_total, memory_used,
                            network_in, network_out, network_in_speed, network_out_speed, online
                     FROM stats
                     WHERE host_id = ? AND timestamp BETWEEN ? AND ?
                     ORDER BY timestamp ASC
                     LIMIT ?"
                )?;

                let stats = stats_stmt.query_map(params![host_id, start_time, end_time, max_points], |row| {
                    Ok(HostStatRecord {
                        timestamp: row.get(0)?,
                        cpu: row.get(1)?,
                        memory_total: row.get(2)?,
                        memory_used: row.get(3)?,
                        network_in: row.get(4)?,
                        network_out: row.get(5)?,
                        network_in_speed: row.get(6)?,
                        network_out_speed: row.get(7)?,
                        online: row.get(8)?,
                        alias: host_alias.clone(),
                        disks: Vec::new(),
                    })
                })?;

                let mut host_stats = Vec::new();
                for stat_result in stats {
                    host_stats.push(stat_result?);
                }

                // 如果有统计数据，获取磁盘数据
                if !host_stats.is_empty() {
                    // 获取该主机在时间范围内的所有磁盘数据
                    let mut disks_stmt = conn.prepare(
                        "SELECT timestamp, mount_point, disk_total, disk_used
                         FROM disk_stats
                         WHERE host_id = ? AND timestamp BETWEEN ? AND ?
                         ORDER BY timestamp ASC, mount_point ASC"
                    )?;

                    let disks = disks_stmt.query_map(params![host_id, start_time, end_time], |row| {
                        Ok(DiskRecord {
                            timestamp: row.get(0)?,
                            mount_point: row.get(1)?,
                            total: row.get(2)?,
                            used: row.get(3)?,
                        })
                    })?;

                    // 将磁盘数据添加到对应的统计记录中
                    let mut disk_records = Vec::new();
                    for disk_result in disks {
                        disk_records.push(disk_result?);
                    }

                    // 按时间戳将磁盘数据分配到对应的统计记录
                    for stat in &mut host_stats {
                        let stat_disks: Vec<_> = disk_records.iter()
                            .filter(|disk| disk.timestamp == stat.timestamp)
                            .cloned()
                            .collect();

                        stat.disks = stat_disks;
                    }
                }

                result.insert(host_name, host_stats);

        }
    }

        Ok(result)
    }

    pub fn aggregate_data(&self, interval_minutes: i64) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();

        // 获取最新的聚合时间戳 - 使用conn查询
        let last_agg_time: Option<i64> = {
            let mut stmt = conn.prepare(
                "SELECT MAX(timestamp) FROM aggregated_stats WHERE interval_minutes = ?"
            )?;
            stmt.query_row(params![interval_minutes], |row| row.get(0)).ok()
        };

        // 如果没有聚合记录，从最早的数据开始 - 使用conn查询
        let start_time = if let Some(time) = last_agg_time {
            time
        } else {
            let mut stmt = conn.prepare("SELECT min(timestamp) FROM stats limit 1")?;
            stmt.query_row([], |row| row.get::<_, i64>(0)).unwrap_or(0)
        };

        // 计算当前时间对齐到interval_minutes的时间点
        let now = Utc::now().timestamp();
        let interval_seconds = interval_minutes * 60;
        let end_time = (now / interval_seconds) * interval_seconds;

        // 如果没有新数据需要聚合，直接返回
        if start_time >= end_time {
            return Ok(());
        }

        // 获取所有主机 - 使用conn查询
        let hosts: Vec<(i64, String)> = {
            let mut hosts_stmt = conn.prepare("SELECT id, name FROM hosts")?;
            let hosts_iter = hosts_stmt.query_map([], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })?;

            let mut result = Vec::new();
            for host_result in hosts_iter {
                result.push(host_result?);
            }
            result
        };

        // 第一阶段：收集所有需要聚合的数据
        let mut aggregated_data = Vec::new();
        let mut aggregated_disk_data = Vec::new();

        for (host_id, _host_name) in hosts {
            let mut current_time = start_time;
            while current_time < end_time {
                let period_end = current_time + interval_seconds;

                // 聚合主机统计数据 - 使用conn查询
                let row_opt = {
                    let mut agg_stmt = conn.prepare(
                        "SELECT
                            AVG(cpu_usage) as avg_cpu,
                            AVG(memory_total) as avg_memory_total,
                            AVG(memory_used) as avg_memory_used,
                            MAX(network_in) as max_network_in,
                            MAX(network_out) as max_network_out,
                            AVG(network_in_speed) as avg_in_speed,
                            AVG(network_out_speed) as avg_out_speed,
                            MAX(online) as was_online
                         FROM stats
                         WHERE host_id = ? AND timestamp >= ? AND timestamp < ?"
                    )?;

                    agg_stmt.query_row(params![host_id, current_time, period_end], |row| {
                        Ok((
                            row.get::<_, Option<f64>>(0)?,
                            row.get::<_, Option<f64>>(1)?,
                            row.get::<_, Option<f64>>(2)?,
                            row.get::<_, Option<i64>>(3)?,
                            row.get::<_, Option<i64>>(4)?,
                            row.get::<_, Option<f64>>(5)?,
                            row.get::<_, Option<f64>>(6)?,
                            row.get::<_, Option<bool>>(7)?,
                        ))
                    }).ok()
                };

                if let Some((cpu, mem_total, mem_used, net_in, net_out, in_speed, out_speed, online)) = row_opt {
                    if cpu.is_some() || mem_total.is_some() {
                        aggregated_data.push((
                            host_id,
                            current_time,
                            interval_minutes,
                            cpu.unwrap_or(0.0),
                            mem_total.unwrap_or(0.0),
                            mem_used.unwrap_or(0.0),
                            net_in.unwrap_or(0),
                            net_out.unwrap_or(0),
                            in_speed.unwrap_or(0.0),
                            out_speed.unwrap_or(0.0),
                            online.unwrap_or(false),
                        ));
                    }

                    // 聚合磁盘数据 - 使用conn查询
                    let mut disk_stmt = conn.prepare(
                        "SELECT
                            mount_point,
                            AVG(disk_total) as avg_total,
                            AVG(disk_used) as avg_used
                         FROM disk_stats
                         WHERE host_id = ? AND timestamp >= ? AND timestamp < ?
                         GROUP BY mount_point"
                    )?;

                    let disks = disk_stmt.query_map(params![host_id, current_time, period_end], |row| {
                        Ok((
                            row.get::<_, String>(0)?,
                            row.get::<_, f64>(1)?,
                            row.get::<_, f64>(2)?,
                        ))
                    })?;

                    for disk_result in disks {
                        let (mount_point, total, used) = disk_result?;
                        aggregated_disk_data.push((
                            host_id,
                            current_time,
                            interval_minutes,
                            mount_point,
                            total,
                            used,
                        ));
                    }
                }

                current_time = period_end;
            }
        }

        // 第二阶段：开始事务并写入所有聚合数据
        let tx = conn.transaction()?;

        // 写入主机聚合数据
        for (host_id, timestamp, interval, cpu, mem_total, mem_used, net_in, net_out, in_speed, out_speed, online) in aggregated_data {
            tx.execute(
                "INSERT OR REPLACE INTO aggregated_stats (
                    host_id, timestamp, interval_minutes, cpu_usage,
                    memory_total, memory_used, network_in, network_out,
                    network_in_speed, network_out_speed, online
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                params![
                    host_id,
                    timestamp,
                    interval,
                    cpu,
                    mem_total,
                    mem_used,
                    net_in,
                    net_out,
                    in_speed,
                    out_speed,
                    online
                ],
            )?;
        }

        // 写入磁盘聚合数据
        for (host_id, timestamp, interval, mount_point, total, used) in aggregated_disk_data {
            tx.execute(
                "INSERT OR REPLACE INTO aggregated_disk_stats (
                    host_id, timestamp, interval_minutes, mount_point, disk_total, disk_used
                ) VALUES (?, ?, ?, ?, ?, ?)",
                params![
                    host_id,
                    timestamp,
                    interval,
                    mount_point,
                    total,
                    used
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }
    // 添加清理旧数据的方法
    pub fn cleanup_old_data(&self, retention_days: i64) -> Result<usize> {
        let mut conn = self.conn.lock().unwrap();  // 修改这里，添加 mut 关键字
        let now = Utc::now().timestamp();
        let cutoff_time = now - (retention_days * 24 * 60 * 60);

        let tx = conn.transaction()?;

        // 删除旧的统计数据
        let stats_deleted = tx.execute(
            "DELETE FROM stats WHERE timestamp < ?",
            params![cutoff_time],
        )?;

        // 删除旧的磁盘数据
        let disks_deleted = tx.execute(
            "DELETE FROM disk_stats WHERE timestamp < ?",
            params![cutoff_time],
        )?;

        tx.commit()?;

        Ok(stats_deleted + disks_deleted)
    }

    pub fn run_scheduled_aggregation(&self) -> Result<()> {
        // 执行5分钟聚合
        self.aggregate_data(5)?;

        // 执行15分钟聚合
        self.aggregate_data(15)?;

        // 执行30分钟聚合
        self.aggregate_data(30)?;

        // 执行60分钟聚合
        self.aggregate_data(60)?;

        self.cleanup_old_data(1)?;

        Ok(())
    }
    // 添加数据库优化方法
    pub fn _optimize(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();  // 修改这里，添加 mut 关键字

        // 运行VACUUM来整理数据库文件
        conn.execute_batch("VACUUM")?;

        // 分析表以优化查询计划
        conn.execute_batch("ANALYZE")?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DiskRecord {
    pub timestamp: i64,  // 添加 timestamp 字段
    pub mount_point: String,
    pub total: i64,
    pub used: i64,
}

#[derive(Debug, Clone)]
pub struct HostStatRecord {
    pub timestamp: i64,
    pub alias: String,
    pub cpu: f64,
    pub memory_total: i64,
    pub memory_used: i64,
    pub network_in: i64,
    pub network_out: i64,
    pub network_in_speed: i64,
    pub network_out_speed: i64,
    pub online: bool,
    pub disks: Vec<DiskRecord>,
}
