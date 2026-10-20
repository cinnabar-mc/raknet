//! Byte-level comparison with go-raknet output recorded in `testdata/golden`.
//!
//! `tools/go-raknet` generates the fixtures and CI checks they are current. A case
//! listed in an expected-divergence table must still differ from Go, so fixing it
//! forces removal of its entry.

use raknet::prelude::*;
use raknet::protocol::codec::RakCodec;
use raknet::protocol::packets::ack::Ack;
use raknet::protocol::packets::connection_request_accepted::ConnectionRequestAccepted;
use raknet::protocol::packets::frame_set::FrameSet;
use raknet::protocol::packets::new_incoming_connection::NewIncomingConnection;
use raknet::protocol::packets::unconnected_pong::UnconnectedPong;
use raknet::protocol::types::frame::Frame;
use raknet::test_support::testdata_dir;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

/// Codec cases known to differ from go-raknet, with the exact Rust decode outcome each must keep.
const CODEC_DIVERGENCES: &[(&str, &str)] = &[
    // Known: system-address parsing rejects what go-raknet tolerates (e4fd2fe).
    ("addr/unknown_family", "err"),
    ("cra/unknown_family_address", "err"),
    ("cra/twenty_one_addresses", "err"),
    ("nic/twenty_one_addresses", "err"),
    // Known: 26.30 BDS LAN-off pong without a length field (fbf9894/703ec90).
    ("pong/bds_no_length_field", "err"),
    // Known: the pong must carry the offline magic, which go-raknet does not check.
    ("pong/bad_magic", "err"),
    // Known: a split flag with count 0 is read as unsplit (403fe0a).
    (
        "frame/split_header_count_zero",
        "ok n=26 rel=3 msg=0 seq=0 ord=0 split=0:0/2/0 payload=706172697479",
    ),
    // New: go-raknet's entry cap admits 8193 entries; Rust stops at 8192.
    ("ack/over_max_entries", "err"),
    // New: go-raknet reads an unknown record type as nothing; Rust reads it as a single.
    ("ack/unknown_record_type", "ok nack=0 seqs=9"),
    // New: payload bit lengths round up to whole bytes; go-raknet truncates.
    (
        "frame/bit_length_not_byte_aligned",
        "ok n=12 rel=0 msg=0 seq=0 ord=0 split=0:0/0/0 payload=010203040506070809",
    ),
];

/// Handshake mismatches known to differ from go-raknet, per scenario.
const HANDSHAKE_DIVERGENCES: &[(&str, &[&str])] = &[
    (
        "listen_plain",
        &[
            // Known: Reply 1 is not padded to the granted MTU (1f3b495); its bytes otherwise match.
            "offline[0] id=0x06: go zero-pads to 1464 bytes, rust sends the same 28 bytes unpadded",
            // Known: accepted-connection system addresses differ (109968c); the rest matches.
            "frame[0] rel=3 msg=0 ord=0 split=0 system addresses: go [127.0.0.1:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0], rust [0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0]",
        ],
    ),
    (
        "listen_cookie",
        &[
            // Known: Reply 1 is not padded to the granted MTU (1f3b495); its bytes otherwise match.
            "offline[0] id=0x06: go zero-pads to 1464 bytes, rust sends the same 32 bytes unpadded",
            // Known: accepted-connection system addresses differ (109968c); the rest matches.
            "frame[0] rel=3 msg=0 ord=0 split=0 system addresses: go [127.0.0.1:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0], rust [0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0,0.0.0.0:0]",
        ],
    ),
];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Decodes fixture hex; masked `xx` bytes become zero.
fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap_or(0))
        .collect()
}

/// Reports whether bytes match fixture hex, where `xx` matches any byte.
fn matches(pattern: &str, bytes: &[u8]) -> bool {
    pattern.len() == bytes.len() * 2
        && bytes.iter().enumerate().all(|(i, b)| {
            &pattern[2 * i..2 * i + 2] == "xx" || pattern[2 * i..2 * i + 2] == format!("{b:02x}")
        })
}

fn render_seqs(seqs: &[u32]) -> String {
    if seqs.is_empty() {
        return "-".into();
    }
    let mut parts = Vec::new();
    let (mut start, mut end) = (seqs[0], seqs[0]);
    for &s in &seqs[1..] {
        if s == end + 1 {
            end = s;
            continue;
        }
        parts.push(if start == end {
            format!("{start}")
        } else {
            format!("{start}-{end}")
        });
        (start, end) = (s, s);
    }
    parts.push(if start == end {
        format!("{start}")
    } else {
        format!("{start}-{end}")
    });
    parts.join(",")
}

fn render_frame(frame: &Frame) -> String {
    format!(
        "rel={} msg={} seq={} ord={} split={}:{}/{}/{} payload={}",
        frame.reliability as u8,
        frame.reliable_index,
        frame.sequence_index,
        frame.order_index,
        u8::from(frame.is_split()),
        frame.split_size,
        frame.split_id,
        frame.split_index,
        hex(&frame.payload)
    )
}

fn render_addrs(addrs: &[SocketAddr]) -> String {
    addrs
        .iter()
        .map(SocketAddr::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

/// Decodes one case with the Rust codec, renders it like the Go side and re-encodes it.
fn rust_codec(kind: &str, bytes: &[u8]) -> (String, Option<Vec<u8>>) {
    fn encode<T: RakCodec>(value: &T) -> Option<Vec<u8>> {
        let mut out = Vec::new();
        value.serialize(&mut out).ok().map(|()| out)
    }
    let mut reader = bytes;
    match kind {
        "frame" => match Frame::deserialize(&mut reader) {
            Ok(frame) => (
                format!(
                    "ok n={} {}",
                    bytes.len() - reader.len(),
                    render_frame(&frame)
                ),
                encode(&frame),
            ),
            Err(_) => ("err".into(), None),
        },
        "datagram" => match FrameSet::deserialize(&mut reader) {
            Ok(set) => {
                let flags = 0x80
                    | (u8::from(set.is_pair) << 4)
                    | (u8::from(set.continuous_send) << 3)
                    | (u8::from(set.needs_b_and_as) << 2);
                let frames: Vec<String> = set.frames.iter().map(render_frame).collect();
                (
                    format!(
                        "ok flags={flags:02x} seq={} frames=[{}]",
                        set.sequence,
                        frames.join("; ")
                    ),
                    encode(&set),
                )
            }
            Err(_) => ("err".into(), None),
        },
        "ack" => match Ack::deserialize(&mut reader) {
            Ok(ack) => (
                format!(
                    "ok nack={} seqs={}",
                    u8::from(ack.is_nack),
                    render_seqs(&ack.sequences)
                ),
                encode(&ack),
            ),
            Err(_) => ("err".into(), None),
        },
        "addr" => match SocketAddr::deserialize(&mut reader) {
            Ok(addr) => (
                format!("ok n={} addr={addr}", bytes.len() - reader.len()),
                encode(&addr),
            ),
            Err(_) => ("err".into(), None),
        },
        "cra" => match ConnectionRequestAccepted::deserialize(&mut reader) {
            Ok(pk) => (
                format!(
                    "ok client={} index={} sys=[{}] ping={} pong={}",
                    pk.client_address,
                    pk.system_index,
                    render_addrs(&pk.system_addresses),
                    pk.request_timestamp,
                    pk.timestamp
                ),
                encode(&pk),
            ),
            Err(_) => ("err".into(), None),
        },
        "nic" => match NewIncomingConnection::deserialize(&mut reader) {
            Ok(pk) => (
                format!(
                    "ok server={} sys=[{}] ping={} pong={}",
                    pk.server_address,
                    render_addrs(&pk.internal_addresses),
                    pk.incoming_timestamp,
                    pk.server_timestamp
                ),
                encode(&pk),
            ),
            Err(_) => ("err".into(), None),
        },
        "pong" => match UnconnectedPong::deserialize(&mut reader) {
            Ok(pk) => (
                format!(
                    "ok ping={} guid={} data={}",
                    pk.timestamp,
                    pk.guid,
                    hex(&pk.message)
                ),
                encode(&pk),
            ),
            Err(_) => ("err".into(), None),
        },
        other => panic!("unknown codec fixture kind {other}"),
    }
}

/// Every codec fixture decodes, renders and re-encodes as go-raknet does, apart from listed divergences.
#[test]
fn codec_matches_go_raknet() {
    let path = testdata_dir().join("golden/codec.txt");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()));
    let mut mismatches = BTreeMap::new();
    let mut names = Vec::new();
    for line in text
        .lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
    {
        let (fields, go) = line.split_once(" => ").expect("case outcome");
        let [kind, name, mode, data] = fields.split(' ').collect::<Vec<_>>()[..] else {
            panic!("malformed fixture line {line}");
        };
        let bytes = unhex(data);
        let (rust, encoded) = rust_codec(kind, &bytes);
        let id = format!("{kind}/{name}");
        names.push(id.clone());
        let mut diff = Vec::new();
        if rust != go {
            diff.push(format!("decode: go `{go}`, rust `{rust}`"));
        }
        if mode == "roundtrip" && encoded.as_deref() != Some(bytes.as_slice()) {
            diff.push(format!(
                "encode: rust {}",
                encoded.map_or("err".into(), |e| hex(&e))
            ));
        }
        if !diff.is_empty() {
            mismatches.insert(id, (rust, diff.join("; ")));
        }
    }
    let mut problems = Vec::new();
    for (case, _) in CODEC_DIVERGENCES {
        assert!(
            names.iter().any(|n| n == case),
            "divergence entry {case} names no fixture"
        );
        if !mismatches.contains_key(*case) {
            problems.push(format!(
                "{case}: now matches go-raknet; remove its divergence entry"
            ));
        }
    }
    for (case, (rust, diff)) in &mismatches {
        match CODEC_DIVERGENCES.iter().find(|(c, _)| c == case) {
            Some((_, expected)) if expected == rust && !diff.contains("encode:") => {}
            Some((_, expected)) => problems.push(format!(
                "{case}: divergence changed; expected rust `{expected}`, now {diff}"
            )),
            None => problems.push(format!("{case}: {diff}")),
        }
    }
    assert!(
        problems.is_empty(),
        "codec parity:\n{}",
        problems.join("\n")
    );
}

/// One recorded datagram from a go-raknet transcript.
#[derive(Clone, Debug)]
struct Line {
    c2s: bool,
    ms: u64,
    trigger: Option<usize>,
    hex: String,
}

fn transcript(name: &str) -> Vec<Line> {
    let path = testdata_dir().join(format!("golden/handshake_{name}.txt"));
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()));
    text.lines()
        .filter(|l| !l.starts_with('#') && !l.is_empty())
        .map(|line| {
            let fields: Vec<&str> = line.split(' ').collect();
            match fields[..] {
                ["c2s", ms, hex] => Line {
                    c2s: true,
                    ms: ms.parse().unwrap(),
                    trigger: None,
                    hex: hex.into(),
                },
                ["s2c", ms, trigger, hex] => Line {
                    c2s: false,
                    ms: ms.parse().unwrap(),
                    trigger: Some(trigger.parse().unwrap()),
                    hex: hex.into(),
                },
                _ => panic!("malformed transcript line {line}"),
            }
        })
        .collect()
}

fn is_ack(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes[0] & 0x80 != 0 && bytes[0] & 0x60 != 0
}

fn is_connected(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes[0] & 0x80 != 0
}

/// Lists a connected datagram's frames with their payload as fixture hex.
fn frames_of(hex_text: &str) -> Vec<(String, String)> {
    let bytes = unhex(hex_text);
    let Ok(set) = FrameSet::deserialize(&mut bytes.as_slice()) else {
        return vec![("undecodable".into(), hex_text.into())];
    };
    // Payloads sit at the end of each frame; recover their fixture hex by offset.
    let mut offset = 4;
    set.frames
        .iter()
        .map(|frame| {
            let end = offset + frame.size_hint();
            let start = end - frame.payload.len();
            offset = end;
            (
                format!(
                    "rel={} msg={} ord={} split={}",
                    frame.reliability as u8,
                    frame.reliable_index,
                    frame.order_index,
                    frame.split_size
                ),
                hex_text[2 * start..2 * end].to_string(),
            )
        })
        .collect()
}

/// Compares the endpoint-under-test side of a transcript, returning readable mismatches.
fn compare(go: &[Line], rust: &[(u64, Vec<u8>)], side_c2s: bool) -> Vec<String> {
    let go: Vec<&Line> = go
        .iter()
        .filter(|l| l.c2s == side_c2s && !is_ack(&unhex(&l.hex)))
        .collect();
    let rust: Vec<&(u64, Vec<u8>)> = rust.iter().filter(|(_, b)| !is_ack(b)).collect();
    let mut out = Vec::new();
    let go_offline: Vec<&&Line> = go
        .iter()
        .filter(|l| !is_connected(&unhex(&l.hex)))
        .collect();
    let rust_offline: Vec<&&(u64, Vec<u8>)> =
        rust.iter().filter(|(_, b)| !is_connected(b)).collect();
    for i in 0..go_offline.len().max(rust_offline.len()) {
        match (go_offline.get(i), rust_offline.get(i)) {
            (Some(g), Some((ms, bytes))) => {
                let id = unhex(&g.hex)[0];
                if let Some(mismatch) = offline_mismatch(&g.hex, bytes) {
                    out.push(format!("offline[{i}] {mismatch}"));
                } else if g.ms != *ms {
                    out.push(format!(
                        "offline[{i}] id={id:#04x}: go at {}ms, rust at {ms}ms",
                        g.ms
                    ));
                }
            }
            (Some(g), None) => out.push(format!(
                "offline[{i}]: rust sent nothing, go sent {}",
                &g.hex[..2]
            )),
            (None, Some((ms, bytes))) => out.push(format!(
                "offline[{i}]: rust sent extra {:#04x} at {ms}ms",
                bytes[0]
            )),
            (None, None) => unreachable!(),
        }
    }
    let go_frames: Vec<(u64, String, String)> = go
        .iter()
        .filter(|l| is_connected(&unhex(&l.hex)))
        .flat_map(|l| frames_of(&l.hex).into_iter().map(|(h, p)| (l.ms, h, p)))
        .collect();
    let rust_frames: Vec<(u64, String, Vec<u8>)> = rust
        .iter()
        .filter(|(_, b)| is_connected(b))
        .flat_map(|(ms, b)| {
            frames_of(&hex(b))
                .into_iter()
                .map(|(h, p)| (*ms, h, unhex(&p)))
        })
        .collect();
    for i in 0..go_frames.len().max(rust_frames.len()) {
        match (go_frames.get(i), rust_frames.get(i)) {
            (Some((gms, gh, gp)), Some((rms, rh, rp))) => {
                let id = rp.first().copied().unwrap_or(0xff);
                if gh != rh {
                    out.push(format!("frame[{i}]: go {gh} {gp}, rust {rh} {}", hex(rp)));
                } else if let Some(mismatch) = payload_mismatch(gp, rp) {
                    out.push(format!("frame[{i}] {gh} {mismatch}"));
                } else if gms != rms {
                    out.push(format!(
                        "frame[{i}] id={id:#04x}: go at {gms}ms, rust at {rms}ms"
                    ));
                }
            }
            (Some((_, gh, gp)), None) => out.push(format!(
                "frame[{i}]: rust sent nothing, go sent {gh} {}",
                &gp[..gp.len().min(8)]
            )),
            (None, Some((rms, rh, rp))) => out.push(format!(
                "frame[{i}]: rust sent extra {rh} {} at {rms}ms",
                hex(&rp[..rp.len().min(4)])
            )),
            (None, None) => unreachable!(),
        }
    }
    out
}

/// Compares an offline datagram exactly, except for go-raknet's zero padding of Reply 1.
fn offline_mismatch(pattern: &str, bytes: &[u8]) -> Option<String> {
    if matches(pattern, bytes) {
        return None;
    }
    let id = bytes.first().copied().unwrap_or(0xff);
    let unpadded = pattern.get(..2 * bytes.len());
    if id == 0x06
        && unpadded.is_some_and(|head| matches(head, bytes))
        && pattern[2 * bytes.len()..].bytes().all(|c| c == b'0')
    {
        return Some(format!(
            "id=0x06: go zero-pads to {} bytes, rust sends the same {} bytes unpadded",
            pattern.len() / 2,
            bytes.len()
        ));
    }
    Some(format!("id={id:#04x}: go {pattern}, rust {}", hex(bytes)))
}

/// Decodes a run of wire addresses, or reports where it stopped.
fn render_address_run(mut bytes: &[u8]) -> String {
    let mut addrs = Vec::new();
    while !bytes.is_empty() {
        match SocketAddr::deserialize(&mut bytes) {
            Ok(addr) => addrs.push(addr.to_string()),
            Err(_) => {
                addrs.push(format!("undecodable {}", hex(bytes)));
                break;
            }
        }
    }
    addrs.join(",")
}

/// Compares a connected payload exactly, except for a connection-accepted system-address list.
///
/// The client address, system index and trailing timestamps still compare byte for byte.
fn payload_mismatch(pattern: &str, bytes: &[u8]) -> Option<String> {
    if matches(pattern, bytes) {
        return None;
    }
    let go = unhex(pattern);
    let generic = || Some(format!("payload: go {pattern}, rust {}", hex(bytes)));
    if go.first() != Some(&0x10) || bytes.first() != Some(&0x10) || bytes.len() < 2 {
        return generic();
    }
    let address = if bytes[1] == 6 { 29 } else { 7 };
    let head = 1 + address + 2;
    let tail = 16;
    if go.len() < head + tail || bytes.len() < head + tail || go[1] != bytes[1] {
        return generic();
    }
    let (go_head, go_tail) = (&pattern[..2 * head], &pattern[pattern.len() - 2 * tail..]);
    if !matches(go_head, &bytes[..head]) || !matches(go_tail, &bytes[bytes.len() - tail..]) {
        return generic();
    }
    Some(format!(
        "system addresses: go [{}], rust [{}]",
        render_address_run(&go[head..go.len() - tail]),
        render_address_run(&bytes[head..bytes.len() - tail])
    ))
}

const CLIENT: &str = "10.0.0.2:50000";
const SERVER: &str = "10.0.0.1:19132";
const LATENCY_MS: u64 = 1;

/// Drives the Rust client against the recorded go-raknet server replies.
fn replay_dial(go: &[Line]) -> Vec<(u64, Vec<u8>)> {
    let (client_addr, server_addr): (SocketAddr, SocketAddr) =
        (CLIENT.parse().unwrap(), SERVER.parse().unwrap());
    let origin = Instant::now();
    let at = |ms: u64| origin + Duration::from_millis(ms);
    let end = go.last().map_or(0, |l| l.ms) + 50;
    let mut client = RakClient::new(RakClientConfig::default(), RakClock::new(origin))
        .unwrap()
        .connect(
            server_addr,
            client_addr,
            ClientGuid::from_raw(0x8877_6655_4433_2211),
            origin,
        )
        .unwrap();
    let mut session: Option<RakSession<Established>> = None;
    let mut sent = Vec::new();
    let mut deliveries: VecDeque<(u64, Vec<u8>)> = VecDeque::new();
    let mut wakes = vec![0u64];
    let mut triggers = 0;
    let mut now_ms = 0;
    while now_ms <= end {
        let now = at(now_ms);
        while deliveries.front().is_some_and(|(ms, _)| *ms <= now_ms) {
            let (_, bytes) = deliveries.pop_front().unwrap();
            client
                .handle(RakClientInput::Datagram(bytes.into(), server_addr, now))
                .unwrap();
        }
        client.handle(RakClientInput::Update(now)).unwrap();
        if let Some(s) = &mut session {
            s.handle(RakSessionInput::Update(now)).unwrap();
        }
        let mut outgoing = Vec::new();
        loop {
            let mut progressed = false;
            while let Some(output) = client.poll() {
                progressed = true;
                match output {
                    RakClientOutput::MtuProbe { id, datagram, .. } => {
                        outgoing.push(datagram.to_vec());
                        client
                            .handle(RakClientInput::MtuProbeResult {
                                id,
                                now,
                                elapsed: Duration::ZERO,
                                too_large: false,
                            })
                            .unwrap();
                    }
                    RakClientOutput::SocketDatagram(bytes, _) => outgoing.push(bytes.to_vec()),
                    RakClientOutput::SessionConnected(s) => session = Some(s),
                    RakClientOutput::SessionDatagram(bytes) => {
                        session
                            .as_mut()
                            .unwrap()
                            .handle(RakSessionInput::Datagram(bytes, now))
                            .unwrap();
                    }
                    RakClientOutput::Wait(wait) => wakes.push(now_ms + wait.as_millis() as u64),
                    RakClientOutput::SessionClosing(_) | RakClientOutput::Pong { .. } => {}
                }
            }
            if let Some(s) = &mut session {
                while let Some(output) = s.poll() {
                    progressed = true;
                    match output {
                        RakSessionOutput::Datagram(bytes, _) => outgoing.push(bytes.to_vec()),
                        RakSessionOutput::Wait(wait) => {
                            wakes.push(now_ms + wait.as_millis() as u64)
                        }
                        RakSessionOutput::Packet(_) | RakSessionOutput::Disconnected(..) => {}
                    }
                }
            }
            if !progressed {
                break;
            }
        }
        for bytes in outgoing {
            if !is_ack(&bytes) {
                for reply in go.iter().filter(|l| l.trigger == Some(triggers)) {
                    deliveries.push_back((now_ms + LATENCY_MS, unhex(&reply.hex)));
                }
                triggers += 1;
            }
            sent.push((now_ms, bytes));
        }
        wakes.retain(|&ms| ms > now_ms);
        let next = wakes
            .iter()
            .copied()
            .chain(deliveries.front().map(|(ms, _)| *ms))
            .min();
        now_ms = next.unwrap_or(now_ms + 1).max(now_ms + 1);
    }
    sent
}

/// Drives the Rust server with the recorded scripted client, adopting its cookie.
fn replay_listen(go: &[Line], cookies: bool) -> Vec<(u64, Vec<u8>)> {
    let (client_addr, server_addr): (SocketAddr, SocketAddr) =
        (CLIENT.parse().unwrap(), SERVER.parse().unwrap());
    let origin = Instant::now();
    let config = RakServerConfig {
        guid: 0x0102_0304_0506_0708,
        cookies,
        ..Default::default()
    };
    let mut server = RakServer::new(config, server_addr, RakClock::new(origin), origin).unwrap();
    let mut session: Option<RakSession<Established>> = None;
    let mut sent = Vec::new();
    let mut cookie: Option<[u8; 4]> = None;
    let end = go.last().map_or(0, |l| l.ms) + 50;
    let mut inputs: VecDeque<&Line> = go.iter().filter(|l| l.c2s).collect();
    let mut wake = 0;
    for now_ms in 0..=end {
        let now = origin + Duration::from_millis(now_ms);
        while inputs.front().is_some_and(|l| l.ms <= now_ms) {
            let line = inputs.pop_front().unwrap();
            let mut bytes = unhex(&line.hex);
            if let (Some(cookie), Some(at)) = (cookie, line.hex.find("xxxxxxxx")) {
                bytes[at / 2..at / 2 + 4].copy_from_slice(&cookie);
            }
            server
                .handle(RakServerInput::Datagram(bytes.into(), client_addr, now))
                .unwrap();
        }
        if now_ms >= wake {
            server.handle(RakServerInput::Update(now)).unwrap();
            if let Some(s) = &mut session {
                s.handle(RakSessionInput::Update(now)).unwrap();
            }
            wake = now_ms + 1;
        }
        loop {
            let mut progressed = false;
            while let Some(output) = server.poll() {
                progressed = true;
                match output {
                    RakServerOutput::SocketDatagram(bytes, _) => {
                        if bytes[0] == 0x06 && bytes.len() >= 30 && bytes[25] == 1 {
                            cookie = Some(bytes[26..30].try_into().unwrap());
                        }
                        sent.push((now_ms + LATENCY_MS, bytes.to_vec()));
                    }
                    RakServerOutput::SessionConnected(s) => session = Some(s),
                    RakServerOutput::SessionDatagram(bytes, _) => {
                        session
                            .as_mut()
                            .unwrap()
                            .handle(RakSessionInput::Datagram(bytes, now))
                            .unwrap();
                    }
                    RakServerOutput::SessionClosing(_) => {}
                }
            }
            if let Some(s) = &mut session {
                while let Some(output) = s.poll() {
                    progressed = true;
                    if let RakSessionOutput::Datagram(bytes, _) = output {
                        sent.push((now_ms + LATENCY_MS, bytes.to_vec()));
                    }
                }
            }
            if !progressed {
                break;
            }
        }
    }
    sent
}

/// Handshake datagrams match go-raknet's for every recorded scenario, apart from listed divergences.
#[test]
fn handshake_matches_go_raknet() {
    // Dial scenarios test the Rust client; listen scenarios the Rust server, with or without cookies.
    let scenarios: [(&str, Option<bool>); 5] = [
        ("dial_ladder_cookie", None),
        ("dial_plain", None),
        ("dial_broken_reply_mtu", None),
        ("listen_plain", Some(false)),
        ("listen_cookie", Some(true)),
    ];
    let mut problems = Vec::new();
    for (name, _) in HANDSHAKE_DIVERGENCES {
        assert!(
            scenarios.iter().any(|(s, ..)| s == name),
            "divergence entry {name} names no scenario"
        );
    }
    for (name, listen) in &scenarios {
        let go = transcript(name);
        let actual = match listen {
            None => compare(&go, &replay_dial(&go), true),
            Some(cookies) => compare(&go, &replay_listen(&go, *cookies), false),
        };
        let expected: Vec<String> = HANDSHAKE_DIVERGENCES
            .iter()
            .find(|(s, _)| s == name)
            .map(|(_, d)| d.iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();
        if actual != expected {
            problems.push(format!(
                "{name}:\n  expected divergences {expected:#?}\n  actual {actual:#?}"
            ));
        }
    }
    assert!(
        problems.is_empty(),
        "handshake parity:\n{}",
        problems.join("\n")
    );
}
