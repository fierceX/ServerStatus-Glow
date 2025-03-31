#![allow(unused)]
use anyhow::Result;
use chrono::{Datelike, Local, Timelike};
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use std::borrow::Borrow;
use std::borrow::BorrowMut;
use std::borrow::Cow;
use std::collections::binary_heap::Iter;
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::Host;
use crate::db::Database;
use crate::db::{DiskRecord, HostStatRecord};
use crate::notifier::{Event, Notifier};
use crate::payload::{HostStat, StatsResp};

const SAVE_INTERVAL: u64 = 60;

static STAT_SENDER: OnceCell<SyncSender<Cow<HostStat>>> = OnceCell::new();

pub struct StatsMgr {
    resp_json: Arc<Mutex<String>>,
    stats_data: Arc<Mutex<StatsResp>>,
    db: Arc<Database>, // 数据库字段
}

impl StatsMgr {
    pub fn new() -> Self {
        // 创建数据库连接
        let db = Database::new("stats.db").expect("Failed to initialize database");
        
        Self {
            resp_json: Arc::new(Mutex::new("{}".to_string())),
            stats_data: Arc::new(Mutex::new(StatsResp::new())),
            db: Arc::new(db),
        }
    }

    // 从数据库加载网络数据，替代原来从stats.json加载
    fn load_last_network(&mut self, hosts_map: &mut HashMap<String, Host>) {
        // 从数据库加载最后的网络数据
        if let Ok(last_network_data) = self.db.get_last_network_data() {
            for (name, last_in, last_out) in last_network_data {
                if let Some(srv) = hosts_map.get_mut(&name) {
                    srv.last_network_in = last_in;
                    srv.last_network_out = last_out;
                    trace!("{} => last in/out ({}/{}))", &name, last_in, last_out);
                }
            }
            trace!("load network data from database succ!");
        }
    }

    pub fn init(
        &mut self,
        cfg: &'static crate::config::Config,
        notifies: Arc<Mutex<Vec<Box<dyn Notifier + Send>>>>,
    ) -> Result<()> {
        let hosts_map_base = Arc::new(Mutex::new(cfg.hosts_map.clone()));

        // load last_network_in/out from database
        if let Ok(mut hosts_map) = hosts_map_base.lock() {
            self.load_last_network(&mut hosts_map);
        }

        let (stat_tx, stat_rx) = sync_channel(512);
        STAT_SENDER.set(stat_tx).unwrap();
        let (notifier_tx, notifier_rx) = sync_channel(512);

        let stat_map: Arc<Mutex<HashMap<String, Cow<HostStat>>>> = Arc::new(Mutex::new(HashMap::new()));
        let db = self.db.clone();

        // stat_rx thread
        thread::spawn({
            let hosts_group_map = cfg.hosts_group_map.clone();
            let hosts_map = hosts_map_base.clone();
            let stat_map = stat_map.clone();
            let notifier_tx = notifier_tx.clone();

            move || loop {
                while let Ok(mut stat) = stat_rx.recv() {
                    trace!("recv stat `{:?}", stat);

                    let mut stat_t = stat.to_mut();

                    // group mode
                    if !stat_t.gid.is_empty() {
                        if stat_t.alias.is_empty() {
                            stat_t.alias = stat_t.name.to_string();
                        }

                        if let Ok(mut hosts_map) = hosts_map.lock() {
                            let host = hosts_map.get(&stat_t.name);
                            if host.is_none() || !host.unwrap().gid.eq(&stat_t.gid) {
                                if let Some(group) = hosts_group_map.get(&stat_t.gid) {
                                    // 名称不变，换组了，更新组配置 & last in/out
                                    let mut inst = group.inst_host(&stat_t.name);
                                    if let Some(o) = host {
                                        inst.last_network_in = o.last_network_in;
                                        inst.last_network_out = o.last_network_out;
                                    };
                                    hosts_map.insert(stat_t.name.to_string(), inst);
                                } else {
                                    continue;
                                }
                            }
                        }
                    }

                    //
                    if let Ok(mut hosts_map) = hosts_map.lock() {
                        let host_info = hosts_map.get_mut(&stat_t.name);
                        if host_info.is_none() {
                            error!("invalid stat `{:?}", stat_t);
                            continue;
                        }
                        let info = host_info.unwrap();

                        if info.disabled {
                            continue;
                        }

                        // 补齐
                        if stat_t.location.is_empty() {
                            stat_t.location = info.location.to_string();
                        }
                        if stat_t.host_type.is_empty() {
                            stat_t.host_type = info.r#type.to_owned();
                        }
                        stat_t.notify = info.notify && stat_t.notify;
                        stat_t.pos = info.pos;
                        stat_t.disabled = info.disabled;
                        stat_t.weight += info.weight;
                        stat_t.labels = info.labels.to_owned();

                        // !group
                        if !info.alias.is_empty() {
                            stat_t.alias = info.alias.to_owned();
                        }

                        info.latest_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        stat_t.latest_ts = info.latest_ts;

                        // last_network_in/out
                        if !stat_t.vnstat {
                            let local_now = Local::now();
                            if info.last_network_in == 0
                                || (stat_t.network_in != 0 && info.last_network_in > stat_t.network_in)
                                || (local_now.day() == info.monthstart
                                    && local_now.hour() == 0
                                    && local_now.minute() < 5)
                            {
                                info.last_network_in = stat_t.network_in;
                                info.last_network_out = stat_t.network_out;
                                
                                // 更新数据库中的last_network数据
                                if let Err(e) = db.update_last_network(&stat_t.name, stat_t.network_in, stat_t.network_out) {
                                    error!("Failed to update last network data: {}", e);
                                }
                            } else {
                                stat_t.last_network_in = info.last_network_in;
                                stat_t.last_network_out = info.last_network_out;
                            }
                        }

                        // uptime str
                        let day = (stat_t.uptime as f64 / 3600.0 / 24.0) as i64;
                        if day > 0 {
                            stat_t.uptime_str = format!("{day} 天");
                        } else {
                            stat_t.uptime_str = format!(
                                "{:02}:{:02}:{:02}",
                                (stat_t.uptime as f64 / 3600.0) as i64,
                                (stat_t.uptime as f64 / 60.0) as i64 % 60,
                                stat_t.uptime % 60
                            );
                        }

                        info!("update stat `{:?}", stat_t);
                        if let Ok(mut host_stat_map) = stat_map.lock() {
                            if let Some(pre_stat) = host_stat_map.get(&stat_t.name) {
                                if stat_t.ip_info.is_none() {
                                    stat_t.ip_info = pre_stat.ip_info.to_owned();
                                }

                                if stat_t.notify && (pre_stat.latest_ts + cfg.offline_threshold < stat_t.latest_ts) {
                                    // node up notify
                                    notifier_tx.send((Event::NodeUp, stat.clone()));
                                }
                            }
                            host_stat_map.insert(stat.name.to_string(), stat);
                            
                            // 将数据保存到数据库
                            if let Err(e) = db.save_stat(&stat_t) {
                                error!("Failed to save stat to database: {}", e);
                            }
                        }
                    }
                }
            }
        });

        // timer thread
        thread::spawn({
            let resp_json = self.resp_json.clone();
            let stats_data = self.stats_data.clone();
            let hosts_map = hosts_map_base.clone();
            let stat_map = stat_map.clone();
            let notifier_tx = notifier_tx.clone();
            let db = self.db.clone();
            let mut latest_notify_ts = 0_u64;
            let mut latest_save_ts = 0_u64;
            let mut latest_group_gc = 0_u64;
            let mut latest_alert_check_ts = 0_u64;
            move || loop {
                thread::sleep(Duration::from_millis(500));

                let mut resp = StatsResp::new();
                let now = resp.updated;
                let mut notified = false;

                // group gc
                if latest_group_gc + cfg.group_gc < now {
                    latest_group_gc = now;
                    //
                    if let Ok(mut hosts_map) = hosts_map.lock() {
                        hosts_map.retain(|_, o| o.gid.is_empty() || o.latest_ts + cfg.group_gc >= now);
                    }
                    //
                    if let Ok(mut stat_map) = stat_map.lock() {
                        stat_map.retain(|_, o| o.gid.is_empty() || o.latest_ts + cfg.group_gc >= now);
                    }
                }

                if let Ok(mut host_stat_map) = stat_map.lock() {
                    for (_, stat) in host_stat_map.iter_mut() {
                        if stat.disabled {
                            resp.servers.push(stat.as_ref().clone());
                            continue;
                        }
                        let stat = stat.borrow_mut();
                        let o = stat.to_mut();
                        // 30s 下线
                        if o.latest_ts + cfg.offline_threshold < now {
                            o.online4 = false;
                            o.online6 = false;
                        }

                        // labels
                        const OS_LIST: [&str; 10] = [
                            "centos", "debian", "ubuntu", "arch", "windows", "macos", "pi", "android", "linux", "freebsd"
                        ];
                        if !o.labels.contains("os=") {
                            if let Some(sys_info) = &o.sys_info {
                                let os_r = format!("{} {}",sys_info.os_release.to_lowercase(),sys_info.os_name.to_lowercase());
                                for s in OS_LIST.iter() {
                                    if os_r.contains(s) {
                                        if o.labels.is_empty() {
                                            write!(o.labels, "os={s}");
                                        } else {
                                            write!(o.labels, ";os={s}");
                                        }
                                        break;
                                    }
                                }
                            }
                        }

                        // client notify
                        if o.notify {
                            // notify check /30 s
                            if latest_notify_ts + cfg.notify_interval < now {
                                if o.online4 || o.online6 {
                                    notifier_tx.send((Event::Custom, stat.clone()));
                                } else {
                                    o.disabled = true;
                                    notifier_tx.send((Event::NodeDown, stat.clone()));
                                }
                                notified = true;
                            }
                        }

                        resp.servers.push(stat.as_ref().clone());
                    }
                    if notified {
                        latest_notify_ts = now;
                    }
                }

                resp.servers.sort_by(|a, b| {
                    if a.weight != b.weight {
                        return a.weight.cmp(&b.weight).reverse();
                    }
                    if a.pos != b.pos {
                        return a.pos.cmp(&b.pos);
                    }
                    // same group
                    a.alias.cmp(&b.alias)
                });

                // 定期保存网络数据到数据库
                if latest_save_ts + SAVE_INTERVAL < now {
                    latest_save_ts = now;
                    if !resp.servers.is_empty() {
                        // 不再保存到stats.json，而是确保数据已经保存到数据库
                        trace!("Stats data saved to database");
                    }
                }
                
                if let Ok(mut o) = resp_json.lock() {
                    *o = serde_json::to_string(&resp).unwrap();
                }
                if let Ok(mut o) = stats_data.lock() {
                    *o = resp;
                }
            }
        });

        // notify thread
        thread::spawn(move || loop {
            while let Ok(msg) = notifier_rx.recv() {
                let (e, stat) = msg;
                let notifiers = &*notifies.lock().unwrap();
                trace!("recv notify => {:?}, {:?}", e, stat);
                for notifier in notifiers {
                    trace!("{} notify {:?} => {:?}", notifier.kind(), e, stat);
                    notifier.notify(&e, stat.borrow());
                }
            }
        });

        Ok(())
    }

    pub fn get_stats(&self) -> Arc<Mutex<StatsResp>> {
        self.stats_data.clone()
    }

    pub fn get_stats_json(&self) -> String {
        self.resp_json.lock().unwrap().to_string()
    }

    pub fn report(&self, data: serde_json::Value) -> Result<()> {
        lazy_static! {
            static ref SENDER: SyncSender<Cow<'static, HostStat>> = STAT_SENDER.get().unwrap().clone();
        }

        match serde_json::from_value(data) {
            Ok(stat) => {
                trace!("send stat => {:?} ", stat);
                SENDER.send(Cow::Owned(stat));
            }
            Err(err) => {
                error!("report error => {:?}", err);
            }
        };
        Ok(())
    }

    pub fn get_all_info(&self) -> Result<serde_json::Value> {
        let data = self.stats_data.lock().unwrap();
        let mut resp_json = serde_json::to_value(&*data)?;
        // for skip_serializing
        if let Some(srv_list) = resp_json["servers"].as_array_mut() {
            for (idx, stat) in data.servers.iter().enumerate() {
                if let Some(srv) = srv_list[idx].as_object_mut() {
                    srv.insert("ip_info".into(), serde_json::to_value(stat.ip_info.as_ref())?);
                    srv.insert("sys_info".into(), serde_json::to_value(stat.sys_info.as_ref())?);
                    if !stat.disks.is_empty() {
                        srv.insert("disks".into(), serde_json::to_value(&stat.disks)?);
                    }
                }
            }
        }
        Ok(resp_json)
    }
    
    // 在 StatsMgr 实现中添加
    pub fn get_stats_by_timerange(&self, start_time: i64, end_time: i64) -> Result<serde_json::Value> {
        let stats = self.db.get_stats_by_timerange(start_time, end_time)?;
        
        let mut result = serde_json::json!({
            "updated": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            "servers": []
        });
        
        let servers = result["servers"].as_array_mut().unwrap();
        
        for (host_name, records) in stats {
            if records.is_empty() {
                continue;
            }
            
            // 使用最新记录的基本信息
            let latest = &records[records.len() - 1];
            
            let mut host_data = serde_json::json!({
                "name": host_name,
                "alias": latest.alias,
                "online": latest.online,
                "data_points": records.len(),
                "cpu_history": [],
                "memory_history": [],
                "network_in_history": [],
                "network_out_history": [],
                "disks_history": {}  // 改为对象，每个挂载点一个数组
            });
            
            // 填充历史数据
            let cpu_history = host_data["cpu_history"].as_array_mut().unwrap();
            let memory_history = host_data["memory_history"].as_array_mut().unwrap();
            let network_in_history = host_data["network_in_history"].as_array_mut().unwrap();
            let network_out_history = host_data["network_out_history"].as_array_mut().unwrap();
            let disks_history = host_data["disks_history"].as_object_mut().unwrap();
            
            // 初始化磁盘挂载点
            let mut mount_points = HashSet::new();
            for record in &records {
                for disk in &record.disks {
                    mount_points.insert(disk.mount_point.clone());
                }
            }
            
            // 为每个挂载点创建数组
            for mount_point in &mount_points {
                disks_history.insert(mount_point.clone(), serde_json::json!([]));
            }
            
            for record in &records {
                cpu_history.push(serde_json::json!({
                    "timestamp": record.timestamp,
                    "value": record.cpu
                }));
                
                let mem_percent = if record.memory_total > 0 {
                    (record.memory_used as f64 / record.memory_total as f64) * 100.0
                } else {
                    0.0
                };
                
                memory_history.push(serde_json::json!({
                    "timestamp": record.timestamp,
                    "value": mem_percent,
                    "total": record.memory_total,
                    "used": record.memory_used
                }));
                
                network_in_history.push(serde_json::json!({
                    "timestamp": record.timestamp,
                    "value": record.network_in_speed,
                    "total": record.network_in
                }));
                
                network_out_history.push(serde_json::json!({
                    "timestamp": record.timestamp,
                    "value": record.network_out_speed,
                    "total": record.network_out
                }));
                
                // 处理每个磁盘
                for disk in &record.disks {
                    if let Some(disk_array) = disks_history.get_mut(&disk.mount_point) {
                        let disk_percent = if disk.total > 0 {
                            (disk.used as f64 / disk.total as f64) * 100.0
                        } else {
                            0.0
                        };
                        
                        disk_array.as_array_mut().unwrap().push(serde_json::json!({
                            "timestamp": record.timestamp,
                            "value": disk_percent,
                            "total": disk.total,
                            "used": disk.used
                        }));
                    }
                }
            }
            
            servers.push(host_data);
        }
        
        Ok(result)
    }
}
