// Copyright (c) 2026 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! Drop every TCP packet to and from a local port as it is received, until
//! this process exits.
//!
//! ```text
//! Usage: wfp-blackhole <port>
//! ```
//!
//! The Windows counterpart to the nftables and pf rules `dead-peer.sh`
//! uses on Linux and macOS. Windows Firewall does not filter loopback
//! traffic, but the Windows Filtering Platform it is built on can, given
//! filters of its own.
//!
//! Packets are blocked at the inbound transport layer, after they have
//! left the sender's TCP stack, so the sender waits for an ACK that never
//! comes, exactly as it would for a peer whose host has vanished.
//!
//! The filters live in a sublayer with the highest possible priority, so
//! that no permit in another sublayer, such as Windows Firewall's, is
//! evaluated first and allowed to override them.
//!
//! Everything is added in a dynamic session, which Windows tears down as
//! soon as this process exits, however it exits. Prints `ready` once the
//! filters are in place, then waits to be killed. Requires administrator
//! rights.

#[cfg(windows)]
fn main() {
    windows::main()
}

#[cfg(not(windows))]
fn main() {
    eprintln!("wfp-blackhole only runs on Windows");
    std::process::exit(1);
}

#[cfg(windows)]
mod windows {
    use std::io::Write;
    use std::mem::zeroed;
    use std::ptr::{null, null_mut};

    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::NetworkManagement::WindowsFilteringPlatform::{
        FWP_ACTION_BLOCK, FWP_EMPTY, FWP_MATCH_EQUAL, FWP_UINT8, FWP_UINT16,
        FWPM_CONDITION_IP_LOCAL_PORT, FWPM_CONDITION_IP_PROTOCOL, FWPM_CONDITION_IP_REMOTE_PORT,
        FWPM_FILTER_CONDITION0, FWPM_FILTER0, FWPM_LAYER_INBOUND_TRANSPORT_V4,
        FWPM_SESSION_FLAG_DYNAMIC, FWPM_SESSION0, FWPM_SUBLAYER0, FwpmEngineOpen0, FwpmFilterAdd0,
        FwpmSubLayerAdd0,
    };
    use windows_sys::Win32::System::Rpc::RPC_C_AUTHN_WINNT;
    use windows_sys::core::GUID;

    /// Identifies this tool's sublayer. Arbitrary, but fixed.
    const SUBLAYER_KEY: GUID = GUID::from_u128(0x34d8d69f_0e57_4014_85f9_8d24c98158fb);

    /// IANA protocol number for TCP.
    const IPPROTO_TCP: u8 = 6;

    pub fn main() {
        let port: u16 = match std::env::args().nth(1).and_then(|p| p.parse().ok()) {
            Some(port) => port,
            None => {
                eprintln!("Usage: wfp-blackhole <port>");
                std::process::exit(1);
            }
        };

        // WFP rejects objects without a display name.
        let mut name: Vec<u16> = "zizq dead-peer blackhole"
            .encode_utf16()
            .chain(Some(0))
            .collect();

        unsafe {
            let mut session: FWPM_SESSION0 = zeroed();
            session.flags = FWPM_SESSION_FLAG_DYNAMIC;

            let mut engine: HANDLE = null_mut();
            check(
                "FwpmEngineOpen0",
                FwpmEngineOpen0(null(), RPC_C_AUTHN_WINNT, null(), &session, &mut engine),
            );

            let mut sublayer: FWPM_SUBLAYER0 = zeroed();
            sublayer.subLayerKey = SUBLAYER_KEY;
            sublayer.displayData.name = name.as_mut_ptr();
            sublayer.weight = u16::MAX;
            check(
                "FwpmSubLayerAdd0",
                FwpmSubLayerAdd0(engine, &sublayer, null_mut()),
            );

            // One filter for packets arriving at the port, one for packets
            // arriving from it.
            for port_field in [FWPM_CONDITION_IP_LOCAL_PORT, FWPM_CONDITION_IP_REMOTE_PORT] {
                let mut conditions: [FWPM_FILTER_CONDITION0; 2] = zeroed();

                conditions[0].fieldKey = FWPM_CONDITION_IP_PROTOCOL;
                conditions[0].matchType = FWP_MATCH_EQUAL;
                conditions[0].conditionValue.r#type = FWP_UINT8;
                conditions[0].conditionValue.Anonymous.uint8 = IPPROTO_TCP;

                conditions[1].fieldKey = port_field;
                conditions[1].matchType = FWP_MATCH_EQUAL;
                conditions[1].conditionValue.r#type = FWP_UINT16;
                conditions[1].conditionValue.Anonymous.uint16 = port;

                let mut filter: FWPM_FILTER0 = zeroed();
                filter.displayData.name = name.as_mut_ptr();
                filter.layerKey = FWPM_LAYER_INBOUND_TRANSPORT_V4;
                filter.subLayerKey = SUBLAYER_KEY;
                filter.weight.r#type = FWP_EMPTY;
                filter.numFilterConditions = conditions.len() as u32;
                filter.filterCondition = conditions.as_mut_ptr();
                filter.action.r#type = FWP_ACTION_BLOCK;

                check(
                    "FwpmFilterAdd0",
                    FwpmFilterAdd0(engine, &filter, null_mut(), null_mut()),
                );
            }

            // The engine handle is deliberately never closed: closing it
            // ends the dynamic session and removes the filters.
        }

        println!("ready");
        let _ = std::io::stdout().flush();

        loop {
            std::thread::park();
        }
    }

    /// Exit with the failing call's error code, if it failed.
    fn check(what: &str, rc: u32) {
        if rc != 0 {
            eprintln!("wfp-blackhole: {what} failed: 0x{rc:08x}");
            std::process::exit(1);
        }
    }
}
