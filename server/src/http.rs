use crate::assets::Asset;
use tokio::task::JoinHandle;
use once_cell::sync::OnceCell;
use tokio::runtime::Runtime;
use axum::extract::{Path, Query};
use axum::{
    body::Bytes,
    http::{header, header::HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    Json,
};
use minijinja::context;
use prettytable::Table;
use prost::Message;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt::Write as _;

use stat_common::{server_status::StatRequest, utils::bytes2human};

use crate::auth;
use crate::jinja;
use crate::jwt;
use crate::G_CONFIG;
use crate::G_STATS_MGR;

const KIND: &str = "http";

// 新的接口：只返回实时数据，不需要参数
pub async fn get_stats_json() -> impl IntoResponse {
    // 获取当前状态
    let current_stats = G_STATS_MGR.get().unwrap().get_stats_json();
    
    (
        [(header::CONTENT_TYPE, "application/json")],
        current_stats,
    )
}

// 添加全局变量存储历史数据处理线程池
static HISTORY_RUNTIME: OnceCell<Runtime> = OnceCell::new();

// 初始化历史数据处理线程池
pub fn init_history_runtime(runtime: Runtime) -> Result<(), Runtime> {
    HISTORY_RUNTIME.set(runtime)
}

// 在历史数据查询函数中使用专用线程池
pub async fn get_history_stats(Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let params_clone = params.clone();
    
    // 使用专用线程池处理历史数据查询
    let handle: JoinHandle<([(header::HeaderName, &'static str); 1], String)> = 
        HISTORY_RUNTIME.get().unwrap().spawn(async move {
            let now = chrono::Utc::now().timestamp();
            let start_time = params_clone
                .get("start_time")
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(now - 600);
            
            let end_time = params_clone
                .get("end_time")
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(now);
            
            match G_STATS_MGR.get().unwrap().get_stats_by_timerange(start_time, end_time) {
                Ok(stats) => (
                    [(header::CONTENT_TYPE, "application/json")],
                    serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string()),
                ),
                Err(e) => {
                    error!("Failed to get stats by timerange: {}", e);
                    (
                        [(header::CONTENT_TYPE, "application/json")],
                        json!({
                            "error": format!("Failed to get stats: {}", e),
                            "code": 500
                        }).to_string(),
                    )
                }
            }
        });
    
    match handle.await {
        Ok(response) => response,
        Err(e) => {
            error!("Thread error: {:?}", e);
            (
                [(header::CONTENT_TYPE, "application/json")],
                json!({
                    "error": "Internal server error",
                    "code": 500
                }).to_string(),
            )
        }
    }
}

#[allow(unused)]
pub async fn get_site_config_json() -> impl IntoResponse {
    // TODO
    ([(header::CONTENT_TYPE, "application/json")], "{}")
}

pub async fn admin_api(_claims: jwt::Claims, Path(path): Path<String>, Query(params): Query<HashMap<String, String>>) -> Json<Value> {
    match path.as_str() {
        "stats.json" => {
            // 检查是否有时间范围参数
            if params.contains_key("start_time") || params.contains_key("end_time") {
                let now = chrono::Utc::now().timestamp();
                let start_time = params
                    .get("start_time")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(now - 600); // 默认10分钟前
                
                let end_time = params
                    .get("end_time")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(now);
                
                match G_STATS_MGR.get().unwrap().get_stats_by_timerange(start_time, end_time) {
                    Ok(stats) => return Json(stats),
                    Err(e) => {
                        error!("Failed to get stats by timerange: {}", e);
                        return Json(json!({
                            "error": format!("Failed to get stats: {}", e),
                            "code": 500
                        }));
                    }
                }
            } else {
                let resp = G_STATS_MGR.get().unwrap().get_all_info().unwrap();
                return Json(resp);
            }
        }
        "config.json" => {
            let resp = G_CONFIG.get().unwrap().to_json_value().unwrap();
            return Json(resp);
        }
        _ => {
            //
        }
    }

    Json(json!({ "code": 0, "message": "ok" }))
}

pub fn init_jinja_tpl() -> Result<(), anyhow::Error> {
    let detail_data = Asset::get("/jinja/detail.jinja.html").expect("detail.jinja.html not found");
    let detail_html: String = String::from_utf8(detail_data.data.into()).unwrap();
    jinja::add_template(KIND, "detail", detail_html);

    let map_data = Asset::get("/jinja/map.jinja.html").expect("map.jinja.html not found");
    let map_html: String = String::from_utf8(map_data.data.into()).unwrap();
    jinja::add_template(KIND, "map", map_html);

    let client_init_sh = Asset::get("/jinja/client-init.jinja.sh").expect("client-init.jinja.sh not found");
    let client_init_sh_s: String = String::from_utf8(client_init_sh.data.into()).unwrap();
    jinja::add_template(KIND, "client-init", client_init_sh_s);
    Ok(())
}

pub async fn init_client(uri: Uri, req_header: HeaderMap, Query(params): Query<HashMap<String, String>>) -> Response {
    // dbg!(&params);

    // query args
    let invalid = "".to_string();
    let pass = params.get("pass").unwrap_or(&invalid);
    let uid = params.get("uid").unwrap_or(&invalid);
    let gid = params.get("gid").unwrap_or(&invalid);
    let alias = params.get("alias").unwrap_or(&invalid);

    if pass.is_empty() || (uid.is_empty() && gid.is_empty()) || (uid.is_empty() && alias.is_empty()) {
        return (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED.to_string()).into_response();
    }

    // auth
    let mut auth_ok = false;
    if let Some(cfg) = G_CONFIG.get() {
        if gid.is_empty() {
            auth_ok = cfg.auth(uid, pass)
        } else {
            auth_ok = cfg.group_auth(gid, pass)
        }
    }
    if !auth_ok {
        return (StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED.to_string()).into_response();
    }

    let mut domain = "localhost".to_string();
    let mut scheme = "http".to_string();
    let mut server_url = "".to_string();
    let mut workspace = "".to_string();

    // load deploy config
    if let Some(cfg) = G_CONFIG.get() {
        server_url = cfg.server_url.to_string();
        workspace = cfg.workspace.to_string();
    }
    // build server url
    if server_url.is_empty() {
        if let Some(v) = uri.scheme() {
            scheme = v.to_string();
            debug!("Http Scheme => {}", scheme);
        }
        req_header.get("x-forwarded-proto").map(|v| {
            v.to_str().map(|s| {
                debug!("x-forwarded-proto => {}", s);
                scheme = s.to_string();
            })
        });

        req_header.get("Host").map(|v| {
            v.to_str().map(|host| {
                debug!("Http Host => {}", host);
                domain = host.to_string();
            })
        });
        req_header.get("x-forwarded-host").map(|v| {
            v.to_str().map(|host| {
                debug!("x-forwarded-host => {}", host);
                domain = host.to_string();
            })
        });
        server_url = format!("{scheme}://{domain}/report");
    }

    let debug = params.get("debug").map(|p| p.eq("1")).unwrap_or(false);
    let vnstat = params.get("vnstat").map(|p| p.eq("1")).unwrap_or(false);
    let disable_ping = params.get("ping").map(|p| p.eq("0")).unwrap_or(false);
    let disable_tupd = params.get("tupd").map(|p| p.eq("0")).unwrap_or(false);
    let disable_extra = params.get("extra").map(|p| p.eq("0")).unwrap_or(false);
    let cn = params.get("cn").map(|p| p.eq("1")).unwrap_or(false);
    let weight = params
        .get("weight")
        .map(|p| p.parse::<u64>().unwrap_or(0_u64))
        .unwrap_or(0_u64);
    let vnstat_mr = params
        .get("vnstat-mr")
        .map(|p| p.parse::<u32>().unwrap_or(1_u32))
        .unwrap_or(1_u32);
    let interval = params
        .get("interval")
        .map(|p| p.parse::<u32>().unwrap_or(1_u32))
        .unwrap_or(1_u32);

    let notify = params.get("notify").map(|p| !p.eq("0")).unwrap_or(true);
    let host_type = params.get("type").unwrap_or(&invalid);
    let location = params.get("loc").unwrap_or(&invalid);

    // cm, ct, cu
    let cm = params.get("cm").unwrap_or(&invalid);
    let ct = params.get("ct").unwrap_or(&invalid);
    let cu = params.get("cu").unwrap_or(&invalid);

    let iface = params.get("iface").unwrap_or(&invalid);
    let exclude_iface = params.get("exclude-iface").unwrap_or(&invalid);

    // build client opts
    let mut client_opts = format!(r#"-a "{server_url}" -p "{pass}""#);
    if debug {
        client_opts.push_str(" -d");
    }
    if vnstat {
        client_opts.push_str(" -n");
    }
    if 1 < vnstat_mr && vnstat_mr <= 28 {
        let _ = write!(client_opts, r#" --vnstat-mr {vnstat_mr}"#);
    }
    if disable_ping {
        client_opts.push_str(" --disable-ping");
    }
    if disable_tupd {
        client_opts.push_str(" --disable-tupd");
    }
    if disable_extra {
        client_opts.push_str(" --disable-extra");
    }
    if weight > 0 {
        let _ = write!(client_opts, r#" -w {weight}"#);
    }
    if !gid.is_empty() {
        let _ = write!(client_opts, r#" -g "{gid}""#);
        let _ = write!(client_opts, r#" --alias "{alias}""#);
    }
    if !uid.is_empty() {
        let _ = write!(client_opts, r#" -u "{uid}""#);
    }
    if !notify {
        client_opts.push_str(" --disable-notify");
    }
    if !host_type.is_empty() {
        let _ = write!(client_opts, r#" -t "{host_type}""#);
    }
    if !location.is_empty() {
        let _ = write!(client_opts, r#" --location "{location}""#);
    }
    if !cm.is_empty() && cm.contains(':') {
        let _ = write!(client_opts, r#" --cm "{cm}""#);
    }
    if !ct.is_empty() && ct.contains(':') {
        let _ = write!(client_opts, r#" --ct "{ct}""#);
    }
    if !cu.is_empty() && cu.contains(':') {
        let _ = write!(client_opts, r#" --cu "{cu}""#);
    }

    if !iface.is_empty() {
        let _ = write!(client_opts, r#" --iface "{iface}""#);
    }
    if !exclude_iface.is_empty() {
        let _ = write!(client_opts, r#" --exclude-iface "{exclude_iface}""#);
    }

    if interval > 0 {
        let _ = write!(client_opts, r#" --interval {interval}"#);
    }

    let ip_source = params.get("ip-source").unwrap_or(&invalid);
    if !ip_source.is_empty() {
        let _ = write!(client_opts, r#" --ip-source "{ip_source}""#);
    }

    jinja::render_template(
        KIND,
        "client-init",
        context!(
            pass => pass, uid => uid, gid => gid, alias => alias,
            vnstat => vnstat, weight => weight, cn => cn,
            domain => domain, scheme => scheme,
            server_url => server_url, workspace => workspace,
            client_opts => client_opts,
            pkg_version => env!("CARGO_PKG_VERSION"),
        ),
        false,
    )
    .map(|contents| {
        (
            [
                (header::CONTENT_TYPE, "text/x-sh"),
                (
                    header::CONTENT_DISPOSITION,
                    r#"attachment; filename="ssr-client-init.sh""#,
                ),
            ],
            contents,
        )
            .into_response()
    })
    .unwrap_or(
        //
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::INTERNAL_SERVER_ERROR.to_string(),
        )
            .into_response(),
    )
}

async fn render_jinja_ht_tpl(tag: &'static str) -> Response {
    let o = G_STATS_MGR.get().unwrap().get_all_info().unwrap();

    jinja::render_template(KIND, tag, context!(resp => &o), false)
        .map(|contents| {
            //
            ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], contents).into_response()
        })
        .unwrap_or(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusCode::INTERNAL_SERVER_ERROR.to_string(),
            )
                .into_response(),
        )
}

pub async fn get_map(
    // _claims: jwt::Claims
    _auth: auth::AdminAuth,
) -> Response {
    render_jinja_ht_tpl("map").await
}

pub async fn get_detail(
    // _claims: jwt::Claims
    _auth: auth::AdminAuth,
) -> Response {
    let resp = G_STATS_MGR.get().unwrap().get_stats();
    let o = resp.lock().unwrap();

    let mut table = Table::new();
    table.set_titles(row![
        "#",
        "Id",
        "节点名",
        "位置",
        "在线时间",
        "IP",
        "系统信息",
        "IP信息",
        "存储信息"
    ]);
    for (idx, host) in o.servers.iter().enumerate() {
        let sys_info = host
            .sys_info
            .as_ref()
            .map(|o| {
                let mut s = String::new();
                s.push_str(&format!("version:        {}\n", o.version));
                s.push_str(&format!("host_name:      {}\n", o.host_name));
                s.push_str(&format!("os_name:        {}\n", o.os_name));
                s.push_str(&format!("os_arch:        {}\n", o.os_arch));
                s.push_str(&format!("os_family:      {}\n", o.os_family));
                s.push_str(&format!("os_release:     {}\n", o.os_release));
                s.push_str(&format!("kernel_version: {}\n", o.kernel_version));
                s.push_str(&format!("cpu_num:        {}\n", o.cpu_num));
                s.push_str(&format!("cpu_brand:      {}\n", o.cpu_brand));
                s.push_str(&format!("cpu_vender_id:  {}", o.cpu_vender_id));
                s
            })
            .unwrap_or_default();

        let mut di = String::new();
        if !host.disks.is_empty() {
            let mut t = Table::new();
            t.set_titles(row!["名称", "挂载点", "类型", "总容量", "已用", "可用"]);
            
            // 先显示普通文件系统
            let normal_disks: Vec<_> = host.disks.iter()
                .filter(|disk| disk.file_system.to_lowercase() != "zfs" && !disk.name.starts_with("zpool-"))
                .collect();
            
            for disk in normal_disks {
                t.add_row(row![
                    disk.name,
                    disk.mount_point,
                    disk.file_system,
                    bytes2human(disk.total, 2, host.si),
                    bytes2human(disk.used, 2, host.si),
                    bytes2human(disk.free, 2, host.si),
                ]);
            }
            
            // 如果有 ZFS 存储池，添加一个分隔行
            let zfs_pools: Vec<_> = host.disks.iter()
                .filter(|disk| disk.name.starts_with("zpool-"))
                .collect();
            
            if !zfs_pools.is_empty() {
                t.add_row(row!["--- ZFS 存储池 ---", "---", "---", "---", "---", "---"]);
                
                for pool in zfs_pools {
                    let usage_percent = if pool.total > 0 {
                        format!("{}%", (pool.used as f64 * 100.0 / pool.total as f64).round())
                    } else {
                        "0%".to_string()
                    };
                    
                    t.add_row(row![
                        pool.name.strip_prefix("zpool-").unwrap_or(&pool.name),
                        pool.mount_point,
                        "ZFS",
                        bytes2human(pool.total, 2, host.si),
                        format!("{} ({})",
                            bytes2human(pool.used, 2, host.si),
                            usage_percent
                        ),
                        bytes2human(pool.free, 2, host.si),
                    ]);
                }
            }
            
            di = t.to_string();
        }

        if let Some(ip_info) = &host.ip_info {
            let addrs = [
                ip_info.continent.as_str(),
                ip_info.country.as_str(),
                ip_info.region_name.as_str(),
                ip_info.city.as_str(),
            ]
            .iter()
            .map(|s| s.trim())
            .filter(|&s| !s.is_empty())
            .collect::<Vec<&str>>()
            .join("/");

            let isp = [
                ip_info.isp.as_str(),
                ip_info.org.as_str(),
                ip_info.r#as.as_str(),
                ip_info.asname.as_str(),
            ]
            .iter()
            .map(|s| s.trim())
            .filter(|&s| !s.is_empty())
            .collect::<Vec<&str>>()
            .join("\n");

            table.add_row(row![
                idx.to_string(),
                host.name,
                host.alias,
                host.location,
                host.uptime_str,
                ip_info.query,
                sys_info,
                format!("{addrs}\n{isp}"),
                di
            ]);
        } else {
            table.add_row(row![
                idx.to_string(),
                host.name,
                host.alias,
                host.location,
                host.uptime_str,
                "xx.xx.xx.xx".to_string(),
                sys_info,
                "".to_string(),
                di
            ]);
        }
    }
    // table.printstd();

    jinja::render_template(KIND, "detail", context!(pretty_content => table.to_string()), true)
        .map(|contents| {
            //
            ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], contents).into_response()
        })
        .unwrap_or(
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusCode::INTERNAL_SERVER_ERROR.to_string(),
            )
                .into_response(),
        )
}

// report
pub async fn report(_auth: auth::HostAuth, req_header: HeaderMap, body: Bytes) -> impl IntoResponse {
    let mut json_data: Option<serde_json::Value> = None;

    let content_type_header = req_header.get(header::CONTENT_TYPE);
    let content_type = content_type_header.and_then(|value| value.to_str().ok());
    if let Some(content_type) = content_type {
        if content_type.starts_with("application/octet-stream") {
            if let Ok(stat) = StatRequest::decode(body) {
                match serde_json::to_value(stat) {
                    Ok(v) => {
                        json_data = Some(v);
                    }
                    Err(err) => {
                        error!("Invalid pb data! {:?}", err);
                    }
                }
            }
        } else if content_type.starts_with("application/json") {
            match serde_json::from_slice(&body) {
                Ok(v) => {
                    json_data = Some(v);
                }
                Err(err) => {
                    error!("Invalid json data! {:?}", err);
                }
            }
        } else {
            return StatusCode::UNSUPPORTED_MEDIA_TYPE;
        }
    }

    if json_data.is_none() {
        error!("{}", "Invalid json data!");
        return StatusCode::BAD_REQUEST;
    }

    if let Some(mgr) = G_STATS_MGR.get() {
        if mgr.report(json_data.unwrap()).is_err() {
            return StatusCode::BAD_REQUEST;
        }
    }

    StatusCode::OK
}
