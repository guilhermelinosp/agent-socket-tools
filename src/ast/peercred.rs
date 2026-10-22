//! Peer-credential inspection: authorization independent of the socket
//! connection itself succeeding.
//!
//! The socket file's `0600` permissions (see [`crate::transport`]) already
//! mean only the owning user's processes can *open* the socket at all, so
//! on an unshared, single-user machine this is defense in depth rather
//! than the only thing standing between "connected" and "authorized."
//! Where it earns its keep is anywhere that assumption doesn't hold —
//! shared accounts, containers sharing a mount namespace, or simply not
//! wanting to rely on filesystem permissions alone for an authorization
//! decision. `docs/AST_PROTOCOL.md`'s "Security model" section documents
//! the policy this crate's server actually applies with it (same-uid by
//! default) and the Linux/macOS differences below.

use std::io;
use std::os::unix::io::AsRawFd;
use tokio::net::UnixStream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerCredentials {
    pub uid: u32,
    /// Only available on Linux (`SO_PEERCRED` includes it); macOS's
    /// `LOCAL_PEERCRED` does not report a pid, so this is `None` there.
    pub pid: Option<i32>,
}

#[cfg(target_os = "linux")]
pub fn peer_credentials(stream: &UnixStream) -> io::Result<PeerCredentials> {
    let fd = stream.as_raw_fd();
    let mut cred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;

    // SAFETY: `fd` is a valid, open socket file descriptor for the
    // lifetime of this call (borrowed from `stream`, which outlives it).
    // `cred`/`len` point at a correctly-sized, live local, matching what
    // `getsockopt(SOL_SOCKET, SO_PEERCRED, ...)` expects to write.
    let ret = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut libc::ucred as *mut libc::c_void,
            &mut len,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(PeerCredentials {
        uid: cred.uid,
        pid: Some(cred.pid),
    })
}

// macOS's `<sys/ucred.h>` `struct xucred` and the `LOCAL_PEERCRED`/
// `SOL_LOCAL` constants it's read with aren't exposed by the `libc` crate
// for this target, so they're declared here, matching the system header
// exactly (`cr_version`/`cr_uid`/`cr_ngroups`/`cr_groups[NGROUPS]`,
// `NGROUPS == 16`). This is the one place this crate defines its own FFI
// shape rather than using `libc`'s; `#[repr(C)]` plus the exact field
// order/types from the header is what makes it safe to hand to
// `getsockopt`.
#[cfg(target_os = "macos")]
const SOL_LOCAL: libc::c_int = 0;
#[cfg(target_os = "macos")]
const LOCAL_PEERCRED: libc::c_int = 1;
#[cfg(target_os = "macos")]
const XUCRED_NGROUPS: usize = 16;

#[cfg(target_os = "macos")]
#[repr(C)]
struct XuCred {
    cr_version: libc::c_uint,
    cr_uid: libc::uid_t,
    cr_ngroups: libc::c_short,
    cr_groups: [libc::gid_t; XUCRED_NGROUPS],
}

#[cfg(target_os = "macos")]
pub fn peer_credentials(stream: &UnixStream) -> io::Result<PeerCredentials> {
    let fd = stream.as_raw_fd();
    let mut cred: XuCred = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<XuCred>() as libc::socklen_t;

    // SAFETY: same reasoning as the Linux branch above, against the
    // macOS-specific `XuCred` shape instead of `libc::ucred`.
    let ret = unsafe {
        libc::getsockopt(
            fd,
            SOL_LOCAL,
            LOCAL_PEERCRED,
            &mut cred as *mut XuCred as *mut libc::c_void,
            &mut len,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(PeerCredentials {
        uid: cred.cr_uid,
        pid: None,
    })
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
pub fn peer_credentials(_stream: &UnixStream) -> io::Result<PeerCredentials> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "peer credential inspection is only implemented for Linux and macOS",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn peer_credentials_of_a_local_pair_report_our_own_uid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peercred-test.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let client = UnixStream::connect(&path).await.unwrap();
        let (server_side, _addr) = listener.accept().await.unwrap();

        // SAFETY: `getuid` takes no arguments and cannot fail.
        let our_uid = unsafe { libc::getuid() };

        let from_server_side = peer_credentials(&server_side).unwrap();
        let from_client_side = peer_credentials(&client).unwrap();
        assert_eq!(from_server_side.uid, our_uid);
        assert_eq!(from_client_side.uid, our_uid);
    }
}
