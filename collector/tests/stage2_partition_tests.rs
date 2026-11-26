use std::path::PathBuf;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use tokyo_l2_collector::config::CollectorConfig;
use tokyo_l2_collector::live::run_live_collector;
use tokyo_l2_collector::storage::{
    read_tsv_zst_records, HourlyTsvZstWriter, PartitionManifest, WriterOptions,
};
use tokyo_l2_collector::types::{CanonicalRecord, ConnId, ExchangeId, RecordKind};

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;

fn mk_rec(wall_ns: i64, mono_ns: u64, conn: ConnId, epoch: u64, payload: &str) -> CanonicalRecord {
    CanonicalRecord {
        recv_wall_ns: wall_ns,
        recv_mono_ns: mono_ns,
        conn_id: conn,
        epoch,
        rx_seq: 0,
        kind: RecordKind::Data,
        payload: payload.to_string(),
    }
}

#[test]
fn test_config_partition_minutes_defaults_validation_and_trial_15m() {
    // 1. Legacy config without partition_minutes / late_frame_grace_ms defaults to 60m / 2000ms.
    let example_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("specs/config.example.json");
    let raw = std::fs::read_to_string(&example_path).expect("read config.example.json");
    let mut obj: Value = serde_json::from_str(&raw).expect("parse example json");
    obj["deployment"]
        .as_object_mut()
        .unwrap()
        .remove("partition_minutes");
    obj["deployment"]
        .as_object_mut()
        .unwrap()
        .remove("late_frame_grace_ms");
    let cfg: CollectorConfig =
        serde_json::from_value(obj.clone()).expect("deserialize legacy config without partition_minutes");
    assert_eq!(cfg.deployment.partition_minutes, 60);
    assert_eq!(cfg.deployment.late_frame_grace_ms, 2000);
    assert!(cfg.validate().is_ok(), "legacy defaulted config must validate");

    // 2. All allowed partition_minutes {5, 15, 30, 60} pass validation.
    for valid_pm in [5u32, 15, 30, 60] {
        let mut c = cfg.clone();
        c.deployment.partition_minutes = valid_pm;
        assert!(
            c.validate().is_ok(),
            "partition_minutes={} should be valid",
            valid_pm
        );
    }

    // 3. Invalid partition_minutes values fail validation.
    for bad_pm in [0u32, 1, 10, 20, 45, 120] {
        let mut c = cfg.clone();
        c.deployment.partition_minutes = bad_pm;
        let errs = c
            .validate()
            .expect_err(&format!("partition_minutes={} must fail validation", bad_pm));
        assert!(
            errs.iter().any(|e| e.contains("partition_minutes")),
            "expected partition_minutes error for {}, got {:?}",
            bad_pm,
            errs
        );
    }

    // 4. Excessive late_frame_grace_ms (> half a window) fails validation.
    let mut bad_grace = cfg.clone();
    bad_grace.deployment.partition_minutes = 5;
    bad_grace.deployment.late_frame_grace_ms = 200_000;
    let grace_errs = bad_grace
        .validate()
        .expect_err("excessive late_frame_grace_ms must fail validation");
    assert!(
        grace_errs.iter().any(|e| e.contains("late_frame_grace_ms")),
        "got {:?}",
        grace_errs
    );

    // 5. Explicit 15m trial config validates cleanly with 4 exchanges x 6 symbols = 72 streams.
    let trial_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("specs/config.trial_15m.json");
    let trial = CollectorConfig::load(&trial_path).expect("load config.trial_15m.json");
    assert!(trial.validate().is_ok(), "config.trial_15m.json must validate");
    assert_eq!(trial.deployment.partition_minutes, 15);
    assert_eq!(trial.deployment.late_frame_grace_ms, 2000);
    assert!(trial.exchanges.binance_futures.enabled);
    assert!(trial.exchanges.bybit_linear.enabled);
    assert!(trial.exchanges.bitget_v3.enabled);
    assert!(trial.exchanges.gate_futures.enabled);
    assert_eq!(trial.exchanges.binance_futures.symbols.len(), 6);
    assert_eq!(trial.exchanges.bybit_linear.symbols.len(), 6);
    assert_eq!(trial.exchanges.bitget_v3.symbols.len(), 6);
    assert_eq!(trial.exchanges.gate_futures.symbols.len(), 6);
}

#[test]
fn test_cross_partition_bounded_late_frame_accept_and_reject() {
    let dir = TempDir::new().unwrap();
    // 2026-11-21T00:00:00Z
    let w0_start: i64 = 1_795_219_200 * NS_PER_SEC;
    let w1_start: i64 = w0_start + 15 * NS_PER_MIN;

    let opts = WriterOptions {
        partition_minutes: 15,
        run_id: Some("trial_late_frame".to_string()),
        allow_segmented_restart: true,
        late_frame_grace_ns: 2 * NS_PER_SEC,
        process_start_wall_ns: Some(w0_start),
    };
    let mut writer = HourlyTsvZstWriter::with_options(
        dir.path().to_path_buf(),
        ExchangeId::GateFutures,
        "boot_late_01".to_string(),
        1,
        opts,
    );

    // Tail of W0
    let r1 = mk_rec(w1_start - 500_000_000, 1000, ConnId::A, 1, r#"{"seq":1}"#);
    assert!(writer.write_record(&r1).unwrap().is_none());

    // First frame of W1 dequeued BEFORE last frame of W0!
    let r2_w1 = mk_rec(w1_start + 200_000_000, 1200, ConnId::A, 1, r#"{"seq":2,"w":1}"#);
    assert!(
        writer.write_record(&r2_w1).unwrap().is_none(),
        "W0 must stay in bounded draining state within 2s grace"
    );
    assert!(writer.has_draining_partition());

    // Late tail frame of W0 arrives within 2s grace -> accepted into W0 with exact original timestamps!
    let r3_late_w0 = mk_rec(
        w1_start - 50_000_000,
        1150,
        ConnId::A,
        1,
        r#"{"seq":3,"late":true}"#,
    );
    assert!(writer.write_record(&r3_late_w0).unwrap().is_none());
    assert!(writer.has_draining_partition());

    // Next frame of W1 is past W0's 2s grace -> triggers W0 seal!
    let r4_w1 = mk_rec(
        w1_start + 2_500_000_000,
        1500,
        ConnId::A,
        1,
        r#"{"seq":4,"w":1}"#,
    );
    let sealed_w0 = writer
        .write_record(&r4_w1)
        .unwrap()
        .expect("W0 must seal once wall time passes planned_end + grace");
    assert!(!writer.has_draining_partition());
    assert_eq!(sealed_w0.manifest.partition_minutes, 15);
    assert_eq!(sealed_w0.manifest.window_id, "2026-11-21T00:00Z");
    assert_eq!(sealed_w0.manifest.planned_start_wall_ns, Some(w0_start));
    assert_eq!(sealed_w0.manifest.planned_end_wall_ns, Some(w1_start));
    assert_eq!(sealed_w0.manifest.late_frames_accepted, 1);
    assert_eq!(sealed_w0.manifest.late_frames_rejected, 0);
    assert_eq!(sealed_w0.manifest.data_lines, 2);
    assert_eq!(sealed_w0.manifest.end_wall_ns, Some(w1_start - 50_000_000));

    // Verify exact original timestamps were preserved inside sealed W0
    let w0_recs = read_tsv_zst_records(&sealed_w0.data_path).unwrap();
    assert_eq!(w0_recs.len(), 2);
    assert_eq!(w0_recs[1].0.recv_wall_ns, w1_start - 50_000_000);
    assert_eq!(w0_recs[1].0.recv_mono_ns, 1150);

    // Frame belonging to already-sealed W0 arriving AFTER grace -> auditable LATE_FRAME_REJECTED META in W1
    let r5_too_late = mk_rec(
        w1_start - 10_000_000,
        900,
        ConnId::A,
        1,
        r#"{"seq":5,"too_late":true}"#,
    );
    let outcome_r5 = writer.write_record_with_outcome(&r5_too_late).unwrap();
    assert!(outcome_r5.sealed.is_none());
    assert!(
        !outcome_r5.accepted_as_market_data,
        "expired late frame must NOT be counted as accepted market data"
    );
    assert_eq!(
        outcome_r5.rejected_late_target_window_id.as_deref(),
        Some("2026-11-21T00:00Z")
    );

    let remaining = writer.seal_all().unwrap();
    assert_eq!(remaining.len(), 1);
    let sealed_w1 = &remaining[0];
    assert_eq!(sealed_w1.manifest.window_id, "2026-11-21T00:15Z");
    assert_eq!(sealed_w1.manifest.late_frames_rejected, 1);
    assert_eq!(sealed_w1.manifest.meta_lines, 1);
    assert_eq!(sealed_w1.manifest.data_lines, 2);

    let w1_recs = read_tsv_zst_records(&sealed_w1.data_path).unwrap();
    assert_eq!(w1_recs.len(), 3);
    let rej = &w1_recs[2].0;
    assert_eq!(rej.kind, RecordKind::Meta);
    // Outer wall timestamp stays inside W1 and outer mono clock does NOT regress below 1500
    assert!(rej.recv_wall_ns >= w1_start && rej.recv_wall_ns < w1_start + 15 * NS_PER_MIN);
    assert!(rej.recv_mono_ns >= 1500);
    let rej_json: Value = serde_json::from_str(&rej.payload).unwrap();
    assert_eq!(rej_json["event"], "LATE_FRAME_REJECTED");
    assert_eq!(rej_json["dropped_frames"], 1);
    assert_eq!(rej_json["orig_recv_wall_ns"], w1_start - 10_000_000);
    assert_eq!(rej_json["orig_recv_mono_ns"], 900);
    assert_eq!(rej_json["target_window_id"], "2026-11-21T00:00Z");
    assert_eq!(rej_json["loss_attributed_window_id"], "2026-11-21T00:00Z");
    assert_eq!(rej_json["target_planned_start_wall_ns"], w0_start);
    assert_eq!(rej_json["target_planned_end_wall_ns"], w1_start);
    assert_eq!(rej_json["target_partition_minutes"], 15);
}

#[tokio::test]
async fn test_quiet_exchange_settlement_and_utc_midnight_rollover_with_bg_seal() {
    let dir = TempDir::new().unwrap();
    // 2026-11-21T23:45:00Z -> next window is 2026-11-22T00:00:00Z (UTC midnight boundary!)
    let w_2345: i64 = 1_795_219_200 * NS_PER_SEC + 23 * 3600 * NS_PER_SEC + 45 * NS_PER_MIN;
    let w_0000_next_day: i64 = w_2345 + 15 * NS_PER_MIN;

    let opts = WriterOptions {
        partition_minutes: 15,
        run_id: Some("trial_midnight".to_string()),
        allow_segmented_restart: true,
        late_frame_grace_ns: 2 * NS_PER_SEC,
        process_start_wall_ns: Some(w_2345),
    };
    let mut writer = HourlyTsvZstWriter::with_options(
        dir.path().to_path_buf(),
        ExchangeId::BybitLinear,
        "boot_mid_01".to_string(),
        1,
        opts,
    );
    writer.enable_background_sealing(Arc::new(Semaphore::new(2)));

    // Single frame at 23:45:01Z, then exchange goes completely quiet across UTC midnight
    let r1 = mk_rec(w_2345 + NS_PER_SEC, 100, ConnId::A, 1, r#"{"topic":"orderbook.50.BTCUSDT"}"#);
    writer.write_record(&r1).unwrap();

    // At 00:00:01Z (within 2s grace): must NOT settle yet
    let s_early = writer
        .settle_expired_windows(w_0000_next_day + NS_PER_SEC)
        .unwrap();
    assert!(s_early.is_empty());
    assert!(writer.has_open_partition());

    // At 00:00:03Z (past 2s grace): settles via background task cleanly even with zero new frames!
    let _ = writer
        .settle_expired_windows(w_0000_next_day + 3 * NS_PER_SEC)
        .unwrap();
    assert!(!writer.has_open_partition());

    // Next frame arrives in 2026-11-22T00:00Z while previous window sealed asynchronously
    let r2 = mk_rec(
        w_0000_next_day + 5 * NS_PER_SEC,
        200,
        ConnId::A,
        1,
        r#"{"topic":"orderbook.50.BTCUSDT","day":2}"#,
    );
    writer.write_record(&r2).unwrap();

    let mut all_bg = Vec::new();
    for handle in writer.take_background_seals() {
        all_bg.push(handle.await.unwrap());
    }
    assert_eq!(all_bg.len(), 1);
    let sealed_2345 = all_bg[0].as_ref().expect("background seal of 23:45Z must succeed");
    assert_eq!(sealed_2345.manifest.date_utc, "2026-11-21");
    assert_eq!(sealed_2345.manifest.hour_utc, "23");
    assert_eq!(sealed_2345.manifest.minute_utc, "45");
    assert_eq!(sealed_2345.manifest.window_id, "2026-11-21T23:45Z");
    assert!(sealed_2345.manifest.settled_by_timer);
    assert!(sealed_2345.manifest.coverage_incomplete);
    assert!(sealed_2345.manifest.tail_gap_ms.unwrap_or(0) >= 890_000);

    let final_sealed = writer.seal_all().unwrap();
    assert_eq!(final_sealed.len(), 1);
    let sealed_next = &final_sealed[0];
    assert_eq!(sealed_next.manifest.date_utc, "2026-11-22");
    assert_eq!(sealed_next.manifest.hour_utc, "00");
    assert_eq!(sealed_next.manifest.minute_utc, "00");
    assert_eq!(sealed_next.manifest.window_id, "2026-11-22T00:00Z");
    assert!(sealed_next
        .data_path
        .to_string_lossy()
        .contains("raw/bybit_linear/2026-11-22/"));
}

#[test]
fn test_same_window_restart_immutable_segments_and_unsealed_crash_partial_preserved() {
    let dir = TempDir::new().unwrap();
    let w_start: i64 = 1_795_219_200 * NS_PER_SEC + 75 * NS_PER_MIN; // 2026-11-21T01:15:00Z

    let mk_opts = |start_ns: i64| WriterOptions {
        partition_minutes: 15,
        run_id: Some("trial_restart".to_string()),
        allow_segmented_restart: true,
        late_frame_grace_ns: 2 * NS_PER_SEC,
        process_start_wall_ns: Some(start_ns),
    };

    // Boot 1: writes and seals cleanly mid-window
    let mut w1 = HourlyTsvZstWriter::with_options(
        dir.path().to_path_buf(),
        ExchangeId::BitgetV3,
        "boot_r1".to_string(),
        1,
        mk_opts(w_start + 120 * NS_PER_SEC),
    );
    w1.write_record(&mk_rec(w_start + 121 * NS_PER_SEC, 10, ConnId::A, 1, r#"{"b":1}"#))
        .unwrap();
    let s1 = w1.seal_all().unwrap();
    assert_eq!(s1.len(), 1);
    assert!(s1[0].manifest.is_startup_stub, "started 120s into window -> startup stub");

    // Boot 2: crashes abnormally (dropped without seal_all) leaving an unsealed `.partial` file
    {
        let mut w2 = HourlyTsvZstWriter::with_options(
            dir.path().to_path_buf(),
            ExchangeId::BitgetV3,
            "boot_r2_crash".to_string(),
            1,
            mk_opts(w_start + 300 * NS_PER_SEC),
        );
        w2.write_record(&mk_rec(w_start + 301 * NS_PER_SEC, 20, ConnId::A, 1, r#"{"b":2}"#))
            .unwrap();
        // drop w2 without sealing
    }

    // Boot 3: restarts in the EXACT SAME 15m window (even reusing boot_id collision or new boot_id)
    let mut w3 = HourlyTsvZstWriter::with_options(
        dir.path().to_path_buf(),
        ExchangeId::BitgetV3,
        "boot_r3".to_string(),
        1,
        mk_opts(w_start + 500 * NS_PER_SEC),
    );
    w3.write_record(&mk_rec(w_start + 501 * NS_PER_SEC, 30, ConnId::A, 1, r#"{"b":3}"#))
        .unwrap();
    let s3 = w3.seal_all().unwrap();
    assert_eq!(s3.len(), 1);
    assert_ne!(s1[0].data_path, s3[0].data_path);
    assert_ne!(s1[0].manifest.segment_id, s3[0].manifest.segment_id);

    // Inspect directory: 2 sealed `.tsv.zst`, 2 `.manifest.json`, and 1 untouched `.partial` from Boot 2 crash!
    let day_dir = dir.path().join("raw/bitget_v3/2026-11-21");
    let entries: Vec<String> = std::fs::read_dir(&day_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
        .collect();
    let sealed_zst: Vec<&String> = entries.iter().filter(|n| n.ends_with(".tsv.zst")).collect();
    let manifests: Vec<&String> = entries.iter().filter(|n| n.ends_with(".manifest.json")).collect();
    let partials: Vec<&String> = entries.iter().filter(|n| n.ends_with(".tsv.zst.partial")).collect();
    assert_eq!(sealed_zst.len(), 2, "entries={:?}", entries);
    assert_eq!(manifests.len(), 2, "entries={:?}", entries);
    assert_eq!(partials.len(), 1, "unsealed crash partial must remain traceable: {:?}", entries);

    // Both sealed segments decompress cleanly and preserve independent data
    assert_eq!(read_tsv_zst_records(&s1[0].data_path).unwrap().len(), 1);
    assert_eq!(read_tsv_zst_records(&s3[0].data_path).unwrap().len(), 1);
}

#[tokio::test]
async fn test_live_15m_config_writes_v2_manifest_with_single_ws_epoch() {
    let dir = TempDir::new().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let mut accepted = 0usize;
        while accepted < 2 {
            if let Ok((stream, _)) = listener.accept().await {
                accepted += 1;
                tokio::spawn(async move {
                    if let Ok(mut ws) = accept_async(stream).await {
                        // Read initial subscription messages
                        let _ = tokio::time::timeout(
                            std::time::Duration::from_millis(400),
                            ws.next(),
                        )
                        .await;
                        // Emit valid Gate book_ticker, obu.50 snapshot, and trades frames continuously
                        let mut u = 100u64;
                        for _ in 0..8 {
                            let bt = json!({
                                "channel": "futures.book_ticker",
                                "event": "update",
                                "result": {"s": "BTC_USDT", "b": "90000.1", "B": "5", "a": "90000.2", "A": "4"}
                            });
                            let obu = json!({
                                "channel": "futures.obu",
                                "event": "update",
                                "result": {"s": "ob.BTC_USDT.50", "full": true, "u": u, "b": [["90000.1", "5"]], "a": [["90000.2", "4"]]}
                            });
                            let tr = json!({
                                "channel": "futures.trades",
                                "event": "update",
                                "result": [{"contract": "BTC_USDT", "id": u, "size": "1", "price": "90000.1"}]
                            });
                            if ws.send(Message::Text(bt.to_string().into())).await.is_err() {
                                break;
                            }
                            if ws.send(Message::Text(obu.to_string().into())).await.is_err() {
                                break;
                            }
                            if ws.send(Message::Text(tr.to_string().into())).await.is_err() {
                                break;
                            }
                            u += 1;
                            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    }
                });
            }
        }
    });

    let trial_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("specs/config.trial_15m.json");
    let mut cfg = CollectorConfig::load(&trial_path).unwrap();
    cfg.deployment.data_root = dir.path().to_string_lossy().to_string();
    cfg.deployment.partition_minutes = 15;
    cfg.deployment.late_frame_grace_ms = 2000;
    cfg.exchanges.binance_futures.enabled = false;
    cfg.exchanges.bybit_linear.enabled = false;
    cfg.exchanges.bitget_v3.enabled = false;
    cfg.exchanges.gate_futures.enabled = true;
    cfg.exchanges.gate_futures.ws_url = format!("ws://{}/ws", addr);
    cfg.exchanges.gate_futures.symbols = vec!["BTC_USDT".to_string()];

    run_live_collector(cfg, "trial_15m_boot".to_string(), Some(2))
        .await
        .expect("15m live mock run");
    server.abort();

    // Find sealed 15m manifest under raw/gate_futures/<date>/
    let gate_root = dir.path().join("raw/gate_futures");
    let mut mf_paths = Vec::new();
    for day_entry in std::fs::read_dir(&gate_root).unwrap() {
        let day_dir = day_entry.unwrap().path();
        if day_dir.is_dir() {
            for f in std::fs::read_dir(&day_dir).unwrap() {
                let p = f.unwrap().path();
                if p.file_name().unwrap().to_string_lossy().ends_with(".manifest.json") {
                    mf_paths.push(p);
                }
            }
        }
    }
    assert!(!mf_paths.is_empty(), "expected at least 1 sealed 15m manifest");
    let mf: PartitionManifest =
        serde_json::from_str(&std::fs::read_to_string(&mf_paths[0]).unwrap()).unwrap();
    assert_eq!(mf.schema_version, 2);
    assert_eq!(mf.partition_minutes, 15);
    assert!(mf.file_name.contains("_p15m_"), "file_name={}", mf.file_name);
    assert!(mf.planned_start_wall_ns.is_some());
    assert!(mf.planned_end_wall_ns.is_some());
    assert!(mf.data_lines >= 3);

    // Verify all written records kept connection epoch == 1 (no spurious WS reconnect)
    let data_path = mf_paths[0].with_file_name(&mf.file_name);
    let recs = read_tsv_zst_records(&data_path).unwrap();
    assert!(!recs.is_empty());
    assert!(
        recs.iter().all(|r| r.0.epoch == 1),
        "expected single WS epoch == 1 without spurious reconnects"
    );
}

#[test]
fn test_review_round2_unified_run_id_resolution_and_fail_closed_conflict() {
    let trial_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("specs/config.trial_15m.json");
    let mut cfg = CollectorConfig::load(&trial_path).unwrap();

    // 1. All empty -> error
    cfg.deployment.run_id = None;
    assert!(cfg.resolve_effective_run_id(None, None).is_err());

    // 2. Config deployment.run_id only -> resolves cleanly
    cfg.deployment.run_id = Some("trial_15m_20261121".to_string());
    assert_eq!(
        cfg.resolve_effective_run_id(None, None).unwrap(),
        "trial_15m_20261121"
    );

    // 3. CLI, env, and config all match -> resolves cleanly
    assert_eq!(
        cfg.resolve_effective_run_id(
            Some("trial_15m_20261121"),
            Some("trial_15m_20261121")
        )
        .unwrap(),
        "trial_15m_20261121"
    );

    // 4. Env TOKYO_L2_RUN_ID conflicts with config deployment.run_id -> fails closed!
    let err = cfg
        .resolve_effective_run_id(None, Some("legacy_boot_old"))
        .unwrap_err();
    assert!(err.contains("conflicting run_id"), "err={}", err);
}

#[test]
fn test_review_round2_meta_records_never_stretch_partition_market_data_timestamps() {
    let dir = TempDir::new().unwrap();
    let w_start: i64 = 1_795_219_200 * NS_PER_SEC; // 2026-11-21T00:00:00Z
    let mut writer = HourlyTsvZstWriter::with_options(
        dir.path().to_path_buf(),
        ExchangeId::BinanceFutures,
        "boot_meta_only".to_string(),
        1,
        WriterOptions {
            partition_minutes: 15,
            run_id: Some("trial_meta_check".to_string()),
            allow_segmented_restart: true,
            late_frame_grace_ns: 2 * NS_PER_SEC,
            process_start_wall_ns: Some(w_start),
        },
    );

    // Head META control record at +1s
    writer
        .write_record(&CanonicalRecord {
            recv_wall_ns: w_start + NS_PER_SEC,
            recv_mono_ns: 100,
            conn_id: ConnId::A,
            epoch: 1,
            rx_seq: 1,
            kind: RecordKind::Meta,
            payload: r#"{"event":"CONTROL_HEAD"}"#.to_string(),
        })
        .unwrap();

    // Only 1 real DATA market-data record at +450s (mid-window)
    writer
        .write_record(&mk_rec(
            w_start + 450 * NS_PER_SEC,
            200,
            ConnId::A,
            2,
            r#"{"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT"}}"#,
        ))
        .unwrap();

    // Tail META control record at +899s
    writer
        .write_record(&CanonicalRecord {
            recv_wall_ns: w_start + 899 * NS_PER_SEC,
            recv_mono_ns: 300,
            conn_id: ConnId::A,
            epoch: 1,
            rx_seq: 3,
            kind: RecordKind::Meta,
            payload: r#"{"event":"CONTROL_TAIL"}"#.to_string(),
        })
        .unwrap();

    let sealed = writer.seal_all().unwrap();
    assert_eq!(sealed.len(), 1);
    let mf = &sealed[0].manifest;
    assert_eq!(mf.data_lines, 1);
    assert_eq!(mf.meta_lines, 2);
    // start_wall_ns and end_wall_ns MUST reflect only the real DATA record (+450s), never the META records!
    assert_eq!(mf.start_wall_ns, Some(w_start + 450 * NS_PER_SEC));
    assert_eq!(mf.end_wall_ns, Some(w_start + 450 * NS_PER_SEC));
    assert!(
        mf.coverage_incomplete,
        "1 DATA record stretched by META records must be marked coverage_incomplete"
    );
}

