use anyhow::Result;
use chrono::{DateTime, Local, NaiveDateTime, TimeZone, Utc};
use rusqlite::{params, Connection, Result as SqliteResult};
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
            "CREATE INDEX IF NOT EXISTS idx_stats_host_time ON stats(host_id, timestamp)",
            [],
        )?;
        
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_disk_stats_host_time ON disk_stats(host_id, timestamp)",
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
        let timestamp = Utc::now().timestamp();
        
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
                timestamp,
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
        
        // 保存每个磁盘的数据
        if !stat.disks.is_empty() {
            for disk in &stat.disks {
                tx.execute(
                    "INSERT INTO disk_stats (
                        host_id, timestamp, mount_point, disk_total, disk_used
                    ) VALUES (?, ?, ?, ?, ?)",
                    params![
                        host_id,
                        timestamp,
                        disk.mount_point,
                        disk.total,
                        disk.used
                    ],
                )?;
            }
            
            // 提交事务
            tx.commit()?;
        }
        
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
        
        for host_result in hosts {
            let (host_id, host_name, host_alias) = host_result?;
            
            // 获取该主机在时间范围内的所有统计数据
            let mut stats_stmt = conn.prepare(
                "SELECT timestamp, cpu_usage, memory_total, memory_used, 
                        network_in, network_out, network_in_speed, network_out_speed, online
                 FROM stats
                 WHERE host_id = ? AND timestamp BETWEEN ? AND ?
                 ORDER BY timestamp ASC"
            )?;
            
            let stats = stats_stmt.query_map(params![host_id, start_time, end_time], |row| {
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
                    disks: Vec::new(), // 先创建空的磁盘列表
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
                        timestamp: row.get(0)?,  // 正确使用 timestamp 字段
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
                
                result.insert(host_name, host_stats);
            }
        }
        
        Ok(result)
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