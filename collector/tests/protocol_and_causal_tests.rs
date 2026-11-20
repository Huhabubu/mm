use serde_json::json;
use tempfile::TempDir;
use tokyo_l2_collector::causal_selector::{CausalSelector, SelectorAction};
use tokyo_l2_collector::config::CollectorConfig;
use tokyo_l2_collector::protocol::binance::BinanceValidator;
use tokyo_l2_collector::protocol::bitget::BitgetValidator;
use tokyo_l2_collector::protocol::bybit::BybitValidator;
use tokyo_l2_collector::protocol::gate::GateValidator;
use tokyo_l2_collector::protocol::{FrameVerdict, ProtocolValidator};
use tokyo_l2_collector::storage::{
    decode_tsv_line, encode_tsv_line, read_tsv_zst_records, verify_zstd_and_sha256,
    HourlyTsvZstWriter,
};
use tokyo_l2_collector::types::{
    CanonicalRecord, ConnId, ConnState, EndpointKind, ExchangeId, RawFrame, RecordKind,
};

fn mk_frame(
    wall_ns: i64,
    mono_ns: u64,
    endpoint: EndpointKind,
    conn_id: ConnId,
    epoch: u64,
    payload: &str,
) -> RawFrame {
    RawFrame {
        recv_wall_ns: wall_ns,
        recv_mono_ns: mono_ns,
        exchange: endpoint.exchange(),
        endpoint,
        conn_id,
        epoch,
        payload: payload.to_string(),
    }
}

#[test]
fn test_config_validation_rules() {
    let example = std::path::Path::new("specs/config.example.json");
    let cfg = CollectorConfig::load(example).expect("load example config");
    cfg.validate().expect("example config must be valid");

    let mvp = std::path::Path::new("specs/config.btc_gate_binance_mvp.json");
    let mvp_cfg = CollectorConfig::load(mvp).expect("load mvp config");
    mvp_cfg.validate().expect("mvp config must be valid");

    // Negative: missing Gate X-Gate-Size-Decimal header
    let mut bad_gate = cfg.clone();
    bad_gate.exchanges.gate_futures.required_headers.clear();
    let errs = bad_gate.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("X-Gate-Size-Decimal")));

    // Negative: Binance aggTrade on /public/stream
    let mut bad_bin = cfg.clone();
    bad_bin
        .exchanges
        .binance_futures
        .public_channels
        .push("aggTrade".to_string());
    let errs = bad_bin.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("aggTrade")));

    // Negative: Bitget legacy "trade" topic
    let mut bad_bg = cfg.clone();
    bad_bg
        .exchanges
        .bitget_v3
        .channels
        .push("trade".to_string());
    let errs = bad_bg.validate().unwrap_err();
    assert!(errs.iter().any(|e| e.contains("legacy topic 'trade'")));
}

#[test]
fn test_binance_late_snapshot_refetch_and_pu_continuity() {
    let mut v = BinanceValidator::new_public(vec!["BTCUSDT".to_string()]);
    assert_eq!(v.state(), ConnState::WaitAnchor);

    // Buffer WS depth updates before REST snapshot arrives
    let d1 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1000,"T":999,"s":"BTCUSDT","U":100,"u":105,"pu":99,"b":[["90000.0","1.5"]],"a":[["90000.1","2.0"]]}}"#;
    let d2 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1100,"T":1099,"s":"BTCUSDT","U":106,"u":110,"pu":105,"b":[["90000.0","1.2"]],"a":[]}}"#;
    assert!(matches!(
        v.on_text_frame(d1),
        FrameVerdict::BufferedWaitAnchor { .. }
    ));
    assert!(matches!(
        v.on_text_frame(d2),
        FrameVerdict::BufferedWaitAnchor { .. }
    ));

    // Stale REST snapshot (S=90 < U=100) MUST fail bridge and request refetch
    assert!(v.on_rest_snapshot("BTCUSDT", 90).is_err());
    assert_eq!(v.state(), ConnState::Resync);

    // Valid REST snapshot S=103 bridges d1 (100 <= 103 <= 105) and d2 (pu=105)
    let bridged = v
        .on_rest_snapshot("BTCUSDT", 103)
        .expect("bridge should succeed");
    assert_eq!(bridged, 2);
    assert_eq!(v.state(), ConnState::Live);

    // Next continuous delta pu=110 -> accepted
    let d3 = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1200,"T":1199,"s":"BTCUSDT","U":111,"u":115,"pu":110,"b":[],"a":[["90000.1","0.0"]]}}"#;
    assert!(matches!(
        v.on_text_frame(d3),
        FrameVerdict::AcceptData { .. }
    ));

    // Broken pu=114 != prev_u=115 -> Violation & snapshot refetch
    let d_gap = r#"{"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1300,"T":1299,"s":"BTCUSDT","U":116,"u":120,"pu":114,"b":[],"a":[]}}"#;
    match v.on_text_frame(d_gap) {
        FrameVerdict::Violation {
            needs_snapshot_refetch,
            ..
        } => assert!(needs_snapshot_refetch),
        other => panic!("expected violation, got {:?}", other),
    }
    assert_eq!(v.state(), ConnState::Invalid);

    // aggTrade on /public/stream MUST be rejected; accepted on /market/stream without splitting
    let agg = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1400,"s":"BTCUSDT","a":55,"p":"90000.0","q":"0.25","f":100,"l":102,"T":1399,"m":false}}"#;
    assert!(matches!(v.on_text_frame(agg), FrameVerdict::Violation { .. }));

    let mut m = BinanceValidator::new_market(vec!["BTCUSDT".to_string()]);
    assert!(matches!(m.on_text_frame(agg), FrameVerdict::AcceptData { .. }));
    assert_eq!(m.state(), ConnState::Live);
}

#[test]
fn test_gate_decimal_strings_full_resets_and_strict_sequence() {
    let mut v = GateValidator::new(vec!["SOL_USDT".to_string()]);

    // Snapshot arriving BEFORE subscribe ACK must still be accepted
    let full1 = r#"{"time":1700000000,"time_ms":1700000000100,"channel":"futures.obu","event":"update","result":{"t":1700000000090,"full":true,"s":"ob.SOL_USDT.50","u":500,"b":[["150.01","0.1"],["150.00","1.7"]],"a":[["150.02","2.3"]]}}"#;
    assert!(matches!(
        v.on_text_frame(full1),
        FrameVerdict::AcceptData {
            is_anchor_reset: true,
            ..
        }
    ));
    assert_eq!(v.state(), ConnState::Live);

    // Subscribe ACK after snapshot is benign control frame
    let ack = r#"{"time":1700000000,"channel":"futures.obu","event":"subscribe","error":null,"result":{"status":"success"}}"#;
    assert!(matches!(v.on_text_frame(ack), FrameVerdict::Control { .. }));

    // Incremental U == prev_u + 1 (501 == 500 + 1) with level deletion "0" and decimal "1.7"
    let delta1 = r#"{"time":1700000000,"time_ms":1700000000120,"channel":"futures.obu","event":"update","result":{"t":1700000000110,"s":"ob.SOL_USDT.50","U":501,"u":503,"b":[["150.01","0"],["149.99","1.7"]],"a":[]}}"#;
    assert!(matches!(
        v.on_text_frame(delta1),
        FrameVerdict::AcceptData {
            is_anchor_reset: false,
            ..
        }
    ));

    // Second full=true snapshot resets anchor cleanly (u=600)
    let full2 = r#"{"time":1700000001,"time_ms":1700000001100,"channel":"futures.obu","event":"update","result":{"t":1700000001090,"full":true,"s":"ob.SOL_USDT.50","u":600,"b":[["150.00","0.1"]],"a":[["150.02","1.7"]]}}"#;
    assert!(matches!(
        v.on_text_frame(full2),
        FrameVerdict::AcceptData {
            is_anchor_reset: true,
            ..
        }
    ));

    // Decimal string trade sizes ("0.1", "-1.7") accepted
    let trade_ok = r#"{"time":1700000001,"channel":"futures.trades","event":"update","result":[{"id":9001,"contract":"SOL_USDT","price":"150.01","size":"-1.7","create_time_ms":1700000001150,"is_internal":false}]}"#;
    assert!(matches!(
        v.on_text_frame(trade_ok),
        FrameVerdict::AcceptData { .. }
    ));

    // Integer size (as if X-Gate-Size-Decimal: 1 were missing!) MUST trigger Violation
    let trade_bad_int = r#"{"time":1700000001,"channel":"futures.trades","event":"update","result":[{"id":9002,"contract":"SOL_USDT","price":"150.01","size":1,"create_time_ms":1700000001160}]}"#;
    assert!(matches!(
        v.on_text_frame(trade_bad_int),
        FrameVerdict::Violation { .. }
    ));

    // Sequence break U=605 != prev_u+1=601 MUST trigger Violation + resubscribe
    let mut v2 = GateValidator::new(vec!["SOL_USDT".to_string()]);
    assert!(matches!(v2.on_text_frame(full2), FrameVerdict::AcceptData { .. }));
    let delta_gap = r#"{"time":1700000002,"time_ms":1700000002000,"channel":"futures.obu","event":"update","result":{"t":1700000001990,"s":"ob.SOL_USDT.50","U":605,"u":606,"b":[],"a":[]}}"#;
    match v2.on_text_frame(delta_gap) {
        FrameVerdict::Violation {
            needs_resubscribe, ..
        } => assert!(needs_resubscribe),
        other => panic!("expected sequence violation, got {:?}", other),
    }
}

#[test]
fn test_bybit_u_jumps_u1_restart_l1_repush_and_trade_dedup() {
    let mut v = BybitValidator::new(vec!["BTCUSDT".to_string()]);

    let snap50 = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1700000000000,"data":{"s":"BTCUSDT","b":[["90000.0","1.0"]],"a":[["90000.1","1.0"]],"u":100,"seq":5000}}"#;
    assert!(matches!(
        v.on_text_frame(snap50),
        FrameVerdict::AcceptData {
            is_anchor_reset: true,
            ..
        }
    ));
    assert_eq!(v.state(), ConnState::Live);

    // Jumping u (100 -> 117 -> 142) MUST succeed (never check u == prev_u + 1!)
    let delta_jump = r#"{"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1700000000020,"data":{"s":"BTCUSDT","b":[["90000.0","2.0"]],"a":[],"u":117,"seq":5020}}"#;
    assert!(matches!(
        v.on_text_frame(delta_jump),
        FrameVerdict::AcceptData { .. }
    ));

    // Server restart u == 1 resets book cleanly
    let restart_u1 = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1700000001000,"data":{"s":"BTCUSDT","b":[["90001.0","1.0"]],"a":[["90001.1","1.0"]],"u":1,"seq":5100}}"#;
    match v.on_text_frame(restart_u1) {
        FrameVerdict::AcceptData {
            is_anchor_reset,
            diagnostic,
            ..
        } => {
            assert!(is_anchor_reset);
            assert!(diagnostic
                .unwrap_or_default()
                .contains("bybit_server_restart_u_eq_1_reset"));
        }
        other => panic!("expected restart reset, got {:?}", other),
    }

    // L1 3s silent snapshot re-push with identical u
    let l1_snap = r#"{"topic":"orderbook.1.BTCUSDT","ts":1700000000000,"type":"snapshot","data":{"s":"BTCUSDT","b":[["90001.0","1.0"]],"a":[["90001.1","1.0"]],"u":50,"seq":5100}}"#;
    let l1_repush = r#"{"topic":"orderbook.1.BTCUSDT","ts":1700000003000,"type":"snapshot","data":{"s":"BTCUSDT","b":[["90001.0","1.0"]],"a":[["90001.1","1.0"]],"u":50,"seq":5100}}"#;
    assert!(matches!(v.on_text_frame(l1_snap), FrameVerdict::AcceptData { .. }));
    match v.on_text_frame(l1_repush) {
        FrameVerdict::AcceptData { diagnostic, .. } => {
            assert!(diagnostic.unwrap_or_default().contains("silent_snapshot_repush"));
        }
        other => panic!("expected L1 silent repush accept, got {:?}", other),
    }

    // publicTrade same seq spanning two frames with duplicate trade id 'i'
    let t1 = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1700000004000,"data":[{"T":1700000003999,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"90001.1","i":"tr-1","BT":false,"RPI":false,"seq":8888}]}"#;
    let t2 = r#"{"topic":"publicTrade.BTCUSDT","type":"snapshot","ts":1700000004005,"data":[{"T":1700000003999,"s":"BTCUSDT","S":"Buy","v":"0.1","p":"90001.1","i":"tr-1","BT":false,"RPI":false,"seq":8888},{"T":1700000004000,"s":"BTCUSDT","S":"Sell","v":"0.2","p":"90001.0","i":"tr-2","BT":true,"RPI":true,"seq":8888}]}"#;
    assert!(matches!(
        v.on_text_frame(t1),
        FrameVerdict::AcceptData {
            diagnostic: None,
            ..
        }
    ));
    match v.on_text_frame(t2) {
        FrameVerdict::AcceptData { diagnostic, .. } => {
            assert_eq!(
                diagnostic.as_deref(),
                Some("bybit_duplicate_trade_ids_in_frame=1")
            );
        }
        other => panic!("expected duplicate diagnostic, got {:?}", other),
    }
}

#[test]
fn test_bitget_books50_pre_and_post_cutover_pseq_and_trade_dedup() {
    let mut v = BitgetValidator::new(vec!["BTCUSDT".to_string()]);

    // Pre-2026-12-01 continuous snapshots
    let snap1 = r#"{"action":"snapshot","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["90000.1","1.0"]],"b":[["90000.0","1.0"]],"seq":100,"pseq":0,"ts":"1700000000000"}]}"#;
    let snap2 = r#"{"action":"snapshot","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["90000.1","1.5"]],"b":[["90000.0","1.0"]],"seq":105,"pseq":0,"ts":"1700000000020"}]}"#;
    assert!(matches!(
        v.on_text_frame(snap1),
        FrameVerdict::AcceptData {
            is_anchor_reset: true,
            ..
        }
    ));
    assert!(matches!(
        v.on_text_frame(snap2),
        FrameVerdict::AcceptData {
            is_anchor_reset: true,
            ..
        }
    ));

    // Post-2026-12-01: first update after snapshot satisfies pseq <= seq_snap <= seq (103 <= 105 <= 110)
    let upd1 = r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["90000.1","0"]],"b":[],"seq":110,"pseq":103,"ts":"1700000000040"}]}"#;
    assert!(matches!(
        v.on_text_frame(upd1),
        FrameVerdict::AcceptData {
            is_anchor_reset: false,
            ..
        }
    ));

    // Subsequent update: pseq == prev_seq (110 == 110)
    let upd2 = r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["90000.2","2.0"]],"b":[],"seq":114,"pseq":110,"ts":"1700000000060"}]}"#;
    assert!(matches!(v.on_text_frame(upd2), FrameVerdict::AcceptData { .. }));

    // Standalone pseq == 0 on update CANNOT bridge gap -> Violation!
    let upd_pseq0 = r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[],"b":[["89999.9","1.0"]],"seq":120,"pseq":0,"ts":"1700000000080"}]}"#;
    assert!(matches!(
        v.on_text_frame(upd_pseq0),
        FrameVerdict::Violation {
            needs_resubscribe: true,
            ..
        }
    ));

    // Re-anchor via snapshot, then verify pseq gap triggers Violation
    let snap3 = r#"{"action":"snapshot","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[["90000.2","2.0"]],"b":[["90000.0","1.0"]],"seq":200,"pseq":0,"ts":"1700000000100"}]}"#;
    v.reset();
    assert!(matches!(v.on_text_frame(snap3), FrameVerdict::AcceptData { .. }));
    let upd_bad_bridge = r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"books50","symbol":"BTCUSDT"},"data":[{"a":[],"b":[],"seq":210,"pseq":205,"ts":"1700000000120"}]}"#;
    assert!(matches!(
        v.on_text_frame(upd_bad_bridge),
        FrameVerdict::Violation { .. }
    ));

    // publicTrade dedup by 'i' (not L), no initial snapshot required, preserves isRPI
    let pt1 = r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"publicTrade","symbol":"BTCUSDT"},"data":[{"i":"bg-101","T":"1700000000200","P":"90000.1","v":"0.05","S":"buy","isRPI":false}]}"#;
    let pt2 = r#"{"action":"update","arg":{"instType":"usdt-futures","topic":"publicTrade","symbol":"BTCUSDT"},"data":[{"i":"bg-101","T":"1700000000200","P":"90000.1","v":"0.05","S":"buy","isRPI":true}]}"#;
    assert!(matches!(
        v.on_text_frame(pt1),
        FrameVerdict::AcceptData {
            diagnostic: None,
            ..
        }
    ));
    match v.on_text_frame(pt2) {
        FrameVerdict::AcceptData { diagnostic, .. } => {
            assert_eq!(
                diagnostic.as_deref(),
                Some("bitget_duplicate_trade_ids_in_frame=1")
            );
        }
        other => panic!("expected bitget dup diagnostic, got {:?}", other),
    }
}

#[test]
fn test_ab_causal_switch_gap_and_utc_midnight_rollover() {
    let mut sel = CausalSelector::new(EndpointKind::GatePublic, vec!["BTC_USDT".to_string()], 3, 5);
    let full_u10 = r#"{"time":1795478399,"time_ms":1795478399500,"channel":"futures.obu","event":"update","result":{"t":1795478399490,"full":true,"s":"ob.BTC_USDT.50","u":10,"b":[["90000.0","1.0"]],"a":[["90000.1","1.0"]]}}"#;
    let delta_u11 = r#"{"time":1795478399,"time_ms":1795478399900,"channel":"futures.obu","event":"update","result":{"t":1795478399890,"s":"ob.BTC_USDT.50","U":11,"u":11,"b":[["90000.0","1.5"]],"a":[]}}"#;
    let delta_u12 = r#"{"time":1795478400,"time_ms":1795478400100,"channel":"futures.obu","event":"update","result":{"t":1795478400090,"s":"ob.BTC_USDT.50","U":12,"u":12,"b":[["90000.0","2.0"]],"a":[]}}"#;

    // 2026-11-24 23:59:59.500 UTC = 1795478399_500_000_000 ns (verify hour rollover across 00:00 UTC)
    let t_before_midnight = 1_795_478_399_500_000_000i64;
    let t_after_midnight = 1_795_478_400_100_000_000i64;

    // Both A and B reach LIVE independently
    let o1 = sel.on_frame(mk_frame(
        t_before_midnight,
        1_000,
        EndpointKind::GatePublic,
        ConnId::A,
        1,
        full_u10,
    ));
    assert_eq!(o1.records.len(), 1);
    assert_eq!(sel.conn_state(ConnId::A), ConnState::Live);

    // Standby B receives same snapshot: validates in memory, writes ZERO duplicate disk lines!
    let o2 = sel.on_frame(mk_frame(
        t_before_midnight + 1000,
        2_000,
        EndpointKind::GatePublic,
        ConnId::B,
        1,
        full_u10,
    ));
    assert!(o2.records.is_empty(), "standby must NOT duplicate disk writes");
    assert_eq!(sel.conn_state(ConnId::B), ConnState::Live);

    // Advance both A and B to u=11 before midnight
    let o3 = sel.on_frame(mk_frame(
        t_before_midnight + 400_000_000,
        400_001_000,
        EndpointKind::GatePublic,
        ConnId::A,
        1,
        delta_u11,
    ));
    assert_eq!(o3.records.len(), 1);
    let o4 = sel.on_frame(mk_frame(
        t_before_midnight + 400_001_000,
        400_002_000,
        EndpointKind::GatePublic,
        ConnId::B,
        1,
        delta_u11,
    ));
    assert!(o4.records.is_empty());

    // Planned rotation of A at 00:00:00 UTC -> switches to LIVE B with 0 ms gap!
    let rot = sel.on_planned_rotation(ConnId::A, t_after_midnight, 600_000_000);
    assert_eq!(sel.primary(), ConnId::B);
    assert!(!sel.in_gap());
    assert!(rot.records.iter().any(|r| r.payload.contains("\"SWITCH\"") && r.payload.contains("planned_rotation")));

    // Write records across 00:00 UTC into HourlyTsvZstWriter and verify seamless partition rollover
    let tmp = TempDir::new().unwrap();
    let mut writer =
        HourlyTsvZstWriter::new(tmp.path().to_path_buf(), ExchangeId::GateFutures, "boot1".to_string(), 3);
    for r in o1.records.iter().chain(&o3.records) {
        assert!(writer.write_record(r).unwrap().is_none());
    }
    // First record after 00:00 UTC seals previous hour (23) and opens hour (00)
    let sealed_prev = writer.write_record(&rot.records[0]).unwrap();
    assert!(sealed_prev.is_some(), "crossing 00:00 UTC must atomically seal prior hour");
    let o5 = sel.on_frame(mk_frame(
        t_after_midnight + 50_000_000,
        650_000_000,
        EndpointKind::GatePublic,
        ConnId::B,
        1,
        delta_u12,
    ));
    assert_eq!(o5.records.len(), 1);
    writer.write_record(&o5.records[0]).unwrap();
    writer.seal_current().unwrap();
    assert_eq!(writer.sealed_partitions().len(), 2);

    // Now test failover when standby is NOT LIVE -> MUST emit GAP_START and later GAP_END
    sel.on_disconnect(ConnId::A, t_after_midnight + 100_000_000, 700_000_000, "planned_close");
    assert_eq!(sel.conn_state(ConnId::A), ConnState::Invalid);
    let gap_out = sel.on_disconnect(ConnId::B, t_after_midnight + 200_000_000, 800_000_000, "network_drop");
    assert!(sel.in_gap());
    assert!(gap_out.records.iter().any(|r| r.payload.contains("\"GAP_START\"")));

    // Watchdog > 5s dual outage triggers CRITICAL alert
    let wd = sel.check_silence_watchdog(
        t_after_midnight + 6_000_000_000,
        6_800_000_000,
        30_000_000_000,
    );
    assert!(wd.actions.iter().any(|a| matches!(a, SelectorAction::Alert { severity, .. } if severity == "CRITICAL")));

    // Reconnect A and deliver fresh full=true snapshot -> GAP_END!
    sel.on_reconnect(ConnId::A, 2, 7_000_000_000);
    let recov = sel.on_frame(mk_frame(
        t_after_midnight + 7_000_000_000,
        7_100_000_000,
        EndpointKind::GatePublic,
        ConnId::A,
        2,
        full_u10,
    ));
    assert!(!sel.in_gap());
    assert_eq!(sel.primary(), ConnId::A);
    assert!(recov.records.iter().any(|r| r.payload.contains("\"GAP_END\"")));
}

#[test]
fn test_tsv_raw_byte_roundtrip_and_local_loss() {
    let tmp = TempDir::new().unwrap();
    let mut sel = CausalSelector::new(EndpointKind::BybitPublic, vec!["BTCUSDT".to_string()], 3, 5);
    let mut writer = HourlyTsvZstWriter::new(
        tmp.path().to_path_buf(),
        ExchangeId::BybitLinear,
        "boot_rt".to_string(),
        3,
    );

    let normal_payload = r#"{"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1700000000000,"data":{"s":"BTCUSDT","b":[["90000.0","1.0"]],"a":[["90000.1","1.0"]],"u":10,"seq":1}}"#;
    let tricky_raw = "{\"raw\":\"line1\\nwith\tliteral\ttab\rand\nnewline_中文\"}";

    let rec_normal = CanonicalRecord {
        recv_wall_ns: 1_795_478_400_000_000_000,
        recv_mono_ns: 100,
        conn_id: ConnId::A,
        epoch: 1,
        rx_seq: 1,
        kind: RecordKind::Data,
        payload: normal_payload.to_string(),
    };
    let line_normal = encode_tsv_line(&rec_normal);
    assert!(!line_normal.contains("payload_b64"));
    let (_, restored_normal) = decode_tsv_line(&line_normal).unwrap();
    assert_eq!(restored_normal, normal_payload);

    let rec_tricky = CanonicalRecord {
        recv_wall_ns: 1_795_478_400_000_001_000,
        recv_mono_ns: 200,
        conn_id: ConnId::A,
        epoch: 1,
        rx_seq: 2,
        kind: RecordKind::DataEscaped,
        payload: serde_json::to_string(tricky_raw).unwrap(),
    };

    let loss = sel.on_local_loss(
        3,
        1_795_478_400_000_002_000,
        300,
        "bounded_queue_full",
    );
    assert!(loss
        .actions
        .iter()
        .any(|a| matches!(a, SelectorAction::Alert { .. })));

    writer.write_record(&rec_normal).unwrap();
    writer.write_record(&rec_tricky).unwrap();
    writer.write_record(&loss.records[0]).unwrap();
    let sealed = writer.seal_current().unwrap().expect("sealed partition");

    assert_eq!(sealed.manifest.data_lines, 1);
    assert_eq!(sealed.manifest.data_escaped_lines, 1);
    assert_eq!(sealed.manifest.local_loss_lines, 1);
    assert!(sealed.manifest.zstd_verified);

    let (bytes, sha) = verify_zstd_and_sha256(&sealed.data_path).unwrap();
    assert_eq!(bytes, sealed.manifest.bytes);
    assert_eq!(sha, sealed.manifest.sha256);

    let rows = read_tsv_zst_records(&sealed.data_path).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].1, normal_payload);
    assert_eq!(rows[1].1, tricky_raw);
    assert!(rows[2].1.contains("LOCAL_LOSS"));

    // Corrupting the zstd file must fail verification loudly
    let corrupt_path = tmp.path().join("corrupt.tsv.zst");
    std::fs::write(&corrupt_path, b"not_valid_zstd_bytes").unwrap();
    assert!(verify_zstd_and_sha256(&corrupt_path).is_err());
}

#[test]
fn test_anti_lookahead_truncation_and_future_mutation_bit_identical() {
    let base_wall = 1_795_478_400_000_000_000i64;
    let cutoff_t = base_wall + 5_000_000_000;

    let full_events: Vec<(i64, u64, ConnId, &str, bool)> = vec![
        (
            base_wall + 1_000_000_000,
            1_000_000_000,
            ConnId::A,
            r#"{"time":1,"channel":"futures.obu","event":"update","result":{"t":1,"full":true,"s":"ob.BTC_USDT.50","u":100,"b":[["90000.0","1.0"]],"a":[["90000.1","1.0"]]}}"#,
            false,
        ),
        (
            base_wall + 1_500_000_000,
            1_500_000_000,
            ConnId::B,
            r#"{"time":1,"channel":"futures.obu","event":"update","result":{"t":1,"full":true,"s":"ob.BTC_USDT.50","u":100,"b":[["90000.0","1.0"]],"a":[["90000.1","1.0"]]}}"#,
            false,
        ),
        (
            base_wall + 3_000_000_000,
            3_000_000_000,
            ConnId::A,
            r#"{"time":3,"channel":"futures.obu","event":"update","result":{"t":3,"s":"ob.BTC_USDT.50","U":101,"u":101,"b":[["90000.0","2.5"]],"a":[]}}"#,
            false,
        ),
        (
            base_wall + 5_000_000_000,
            5_000_000_000,
            ConnId::A,
            r#"{"time":5,"channel":"futures.obu","event":"update","result":{"t":5,"s":"ob.BTC_USDT.50","U":102,"u":102,"b":[["89999.9","3.0"]],"a":[]}}"#,
            false,
        ),
        // Events STRICTLY AFTER cutoff_t
        (
            base_wall + 7_000_000_000,
            7_000_000_000,
            ConnId::A,
            r#"{"time":7,"channel":"futures.obu","event":"update","result":{"t":7,"s":"ob.BTC_USDT.50","U":103,"u":103,"b":[["89999.8","4.0"]],"a":[]}}"#,
            false,
        ),
        (
            base_wall + 8_000_000_000,
            8_000_000_000,
            ConnId::A,
            "",
            true, // disconnect A after cutoff_t
        ),
    ];

    let run = |events: &[(i64, u64, ConnId, &str, bool)]| -> Vec<CanonicalRecord> {
        let mut sel =
            CausalSelector::new(EndpointKind::GatePublic, vec!["BTC_USDT".to_string()], 3, 5);
        let mut emitted = Vec::new();
        for &(wall, mono, cid, payload, is_disc) in events {
            let out = if is_disc {
                sel.on_disconnect(cid, wall, mono, "future_fault")
            } else {
                sel.on_frame(mk_frame(wall, mono, EndpointKind::GatePublic, cid, 1, payload))
            };
            emitted.extend(out.records);
        }
        emitted
            .into_iter()
            .filter(|r| r.recv_wall_ns <= cutoff_t)
            .collect()
    };

    // Run 1: full stream
    let r_full = run(&full_events);
    // Run 2: truncated at cutoff_t
    let truncated: Vec<_> = full_events
        .iter()
        .copied()
        .filter(|e| e.0 <= cutoff_t)
        .collect();
    let r_trunc = run(&truncated);
    // Run 3: violently mutated future messages after cutoff_t (gap + huge price jump + disconnect)
    let mut mutated = truncated.clone();
    mutated.push((
        base_wall + 6_000_000_000,
        6_000_000_000,
        ConnId::A,
        r#"{"time":6,"channel":"futures.obu","event":"update","result":{"t":6,"s":"ob.BTC_USDT.50","U":9999,"u":9999,"b":[["1.0","999.0"]],"a":[]}}"#,
        false,
    ));
    mutated.push((
        base_wall + 6_500_000_000,
        6_500_000_000,
        ConnId::B,
        "",
        true,
    ));
    let r_mut = run(&mutated);

    assert_eq!(r_full.len(), 3);
    assert_eq!(r_full, r_trunc, "truncating future altered <= t output!");
    assert_eq!(r_full, r_mut, "mutating future altered <= t output!");
}

#[test]
fn test_no_cross_connection_delta_splicing() {
    // A receives snapshot u=100; B receives snapshot u=100 and advances to u=101.
    // A then receives delta U=102 (skipping 101!). Even though B saw u=101,
    // A's independent validator MUST NOT splice B's u=101 into A; A MUST fault and switch to B!
    let mut sel = CausalSelector::new(EndpointKind::GatePublic, vec!["BTC_USDT".to_string()], 3, 5);
    let snap = r#"{"time":1,"channel":"futures.obu","event":"update","result":{"t":1,"full":true,"s":"ob.BTC_USDT.50","u":100,"b":[["90000.0","1.0"]],"a":[["90000.1","1.0"]]}}"#;
    let d101 = r#"{"time":2,"channel":"futures.obu","event":"update","result":{"t":2,"s":"ob.BTC_USDT.50","U":101,"u":101,"b":[["90000.0","1.5"]],"a":[]}}"#;
    let d102 = r#"{"time":3,"channel":"futures.obu","event":"update","result":{"t":3,"s":"ob.BTC_USDT.50","U":102,"u":102,"b":[["90000.0","2.0"]],"a":[]}}"#;

    let t0 = 1_795_478_400_000_000_000i64;
    sel.on_frame(mk_frame(t0, 10, EndpointKind::GatePublic, ConnId::A, 1, snap));
    sel.on_frame(mk_frame(t0 + 1, 11, EndpointKind::GatePublic, ConnId::B, 1, snap));
    sel.on_frame(mk_frame(t0 + 2, 12, EndpointKind::GatePublic, ConnId::B, 1, d101));

    // A skips 101 and receives 102 -> MUST fail A and failover to LIVE B (never splice!)
    let out = sel.on_frame(mk_frame(t0 + 3, 13, EndpointKind::GatePublic, ConnId::A, 1, d102));
    assert_eq!(sel.conn_state(ConnId::A), ConnState::Invalid);
    assert_eq!(sel.primary(), ConnId::B);
    assert!(out.records.iter().any(|r| r.payload.contains("\"SWITCH\"")));
    assert!(json!(out.records).to_string().contains("sequence break"));
}
