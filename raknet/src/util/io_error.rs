//! UDP error classification shared by socket adapters. See NOTICE for attribution.

use std::error::Error;
use std::io;

/// Classifies an oversized UDP write at the I/O boundary, including wrapped OS errors.
pub fn is_mtu_probe_size_error(error: &io::Error) -> bool {
    #[cfg(unix)]
    let code = Some(libc::EMSGSIZE);
    #[cfg(windows)]
    let code = Some(windows_sys::Win32::Networking::WinSock::WSAEMSGSIZE);
    #[cfg(not(any(unix, windows)))]
    let code: Option<i32> = None;
    code.is_some_and(|code| has_os_error(error, &[code]))
}

/// Classifies connected UDP errors that may be retried during initial offline negotiation.
///
/// Apply a bounded retry policy at the socket edge. This classification alone does
/// not make an error recoverable after negotiation or on other socket operations.
pub fn is_transient_udp_read_error(error: &io::Error) -> bool {
    #[cfg(unix)]
    let codes = &[
        libc::EMSGSIZE,
        libc::ECONNREFUSED,
        libc::EHOSTUNREACH,
        libc::ENETUNREACH,
        libc::ECONNRESET,
    ];
    #[cfg(windows)]
    let codes = &[
        windows_sys::Win32::Networking::WinSock::WSAEMSGSIZE,
        windows_sys::Win32::Networking::WinSock::WSAECONNREFUSED,
        windows_sys::Win32::Networking::WinSock::WSAEHOSTUNREACH,
        windows_sys::Win32::Networking::WinSock::WSAENETUNREACH,
        windows_sys::Win32::Networking::WinSock::WSAENETRESET,
        windows_sys::Win32::Networking::WinSock::WSAECONNRESET,
        windows_sys::Win32::Networking::WinSock::WSAECONNABORTED,
    ];
    #[cfg(not(any(unix, windows)))]
    let codes: &[i32] = &[];
    has_os_error(error, codes)
}

/// Searches wrapped I/O errors without confusing their messages with OS error codes.
fn has_os_error(error: &io::Error, codes: &[i32]) -> bool {
    let mut current: &(dyn Error + 'static) = error;
    loop {
        let next = if let Some(error) = current.downcast_ref::<io::Error>() {
            if error
                .raw_os_error()
                .is_some_and(|code| codes.contains(&code))
            {
                return true;
            }
            error.get_ref().map(|inner| inner as &(dyn Error + 'static))
        } else {
            current.source()
        };
        let Some(next) = next else { return false };
        current = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_error_codes_are_specific_and_survive_wrapping() {
        #[cfg(unix)]
        let codes = [
            libc::EMSGSIZE,
            libc::ECONNREFUSED,
            libc::EHOSTUNREACH,
            libc::ENETUNREACH,
            libc::ECONNRESET,
        ];
        #[cfg(windows)]
        let codes = [
            windows_sys::Win32::Networking::WinSock::WSAEMSGSIZE,
            windows_sys::Win32::Networking::WinSock::WSAECONNREFUSED,
            windows_sys::Win32::Networking::WinSock::WSAEHOSTUNREACH,
            windows_sys::Win32::Networking::WinSock::WSAENETUNREACH,
            windows_sys::Win32::Networking::WinSock::WSAENETRESET,
            windows_sys::Win32::Networking::WinSock::WSAECONNRESET,
            windows_sys::Win32::Networking::WinSock::WSAECONNABORTED,
        ];
        #[cfg(any(unix, windows))]
        for (index, code) in codes.into_iter().enumerate() {
            let error = io::Error::from_raw_os_error(code);
            assert!(is_transient_udp_read_error(&error));
            assert_eq!(is_mtu_probe_size_error(&error), index == 0);
            let wrapped = io::Error::other(io::Error::other(error));
            assert!(is_transient_udp_read_error(&wrapped));
            assert_eq!(is_mtu_probe_size_error(&wrapped), index == 0);
        }
        for error in [
            io::Error::other("unrecognized socket failure"),
            io::Error::from(io::ErrorKind::ConnectionRefused),
        ] {
            assert!(!is_transient_udp_read_error(&error));
            assert!(!is_mtu_probe_size_error(&error));
        }
    }
}
