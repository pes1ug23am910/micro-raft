//! Process signals and a shared, idempotent shutdown request.
//!
//! The first request fixes the drain deadline for every participant. Receivers
//! observe the request without taking ownership of durable writer cancellation.

use std::io;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::Instant;

pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct ShutdownHandle {
    sender: watch::Sender<Option<Instant>>,
}

#[derive(Clone, Debug)]
pub struct ShutdownRx {
    receiver: watch::Receiver<Option<Instant>>,
}

pub fn channel() -> (ShutdownHandle, ShutdownRx) {
    let (sender, receiver) = watch::channel(None);
    (ShutdownHandle { sender }, ShutdownRx { receiver })
}

impl ShutdownHandle {
    /// Request shutdown once; repeated callers cannot extend the drain period.
    pub fn request(&self) -> Instant {
        let now = Instant::now();
        self.sender.send_if_modified(|requested_at| {
            if requested_at.is_none() {
                *requested_at = Some(now);
                true
            } else {
                false
            }
        });
        self.sender.borrow().expect("request time was installed")
    }
}

impl ShutdownRx {
    pub fn requested_at(&self) -> Option<Instant> {
        *self.receiver.borrow()
    }

    pub fn is_requested(&self) -> bool {
        self.requested_at().is_some()
    }

    /// Dropping controllers without a request is not an implicit signal.
    pub async fn requested(&mut self) -> Instant {
        loop {
            if let Some(at) = *self.receiver.borrow_and_update() {
                return at;
            }
            if self.receiver.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignalReason {
    Terminate,
    Interrupt,
    CtrlC,
    CtrlBreak,
}

impl std::fmt::Display for SignalReason {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Terminate => "SIGTERM",
            Self::Interrupt => "SIGINT",
            Self::CtrlC => "Ctrl-C",
            Self::CtrlBreak => "Ctrl-Break",
        })
    }
}

/// Registered OS signal streams, installed before serving requests.
pub struct Signals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(windows)]
    ctrl_c: tokio::signal::windows::CtrlC,
    #[cfg(windows)]
    ctrl_break: tokio::signal::windows::CtrlBreak,
}

pub fn install() -> io::Result<Signals> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        Ok(Signals {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }
    #[cfg(windows)]
    {
        Ok(Signals {
            ctrl_c: tokio::signal::windows::ctrl_c()?,
            ctrl_break: tokio::signal::windows::ctrl_break()?,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "process signal handling is supported on Unix and Windows",
        ))
    }
}

impl Signals {
    pub async fn recv(&mut self) -> io::Result<SignalReason> {
        #[cfg(unix)]
        {
            tokio::select! {
                received = self.terminate.recv() => signal_received(received, SignalReason::Terminate),
                received = self.interrupt.recv() => signal_received(received, SignalReason::Interrupt),
            }
        }
        #[cfg(windows)]
        {
            tokio::select! {
                received = self.ctrl_c.recv() => signal_received(received, SignalReason::CtrlC),
                received = self.ctrl_break.recv() => signal_received(received, SignalReason::CtrlBreak),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported signal platform",
            ))
        }
    }
}

#[cfg(any(unix, windows))]
fn signal_received(received: Option<()>, reason: SignalReason) -> io::Result<SignalReason> {
    received.map(|()| reason).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            format!("{reason} signal stream closed"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_request_is_shared_and_cannot_extend_the_deadline() {
        let (handle, mut receiver) = channel();
        let mut other = receiver.clone();
        assert!(!receiver.is_requested());
        let first = handle.request();
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(handle.clone().request(), first);
        assert_eq!(receiver.requested_at(), Some(first));
        assert_eq!(receiver.requested().await, first);
        assert_eq!(other.requested().await, first);
        assert_eq!(other.clone().requested().await, first);
    }

    #[tokio::test]
    async fn dropping_controller_without_a_request_does_not_invent_shutdown() {
        let (handle, mut receiver) = channel();
        drop(handle);
        assert!(!receiver.is_requested());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), receiver.requested())
                .await
                .is_err()
        );
    }
}
