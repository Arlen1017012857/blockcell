pub mod capability;
pub mod config;
pub mod error;
pub mod message;
pub mod paths;
pub mod types;

pub use capability::{
    CapabilityDescriptor, CapabilityType, CapabilityStatus, CapabilityCost,
    CapabilityLifecycle, ProviderKind, PrivilegeLevel, SurvivalInvariants,
};
pub use config::Config;
pub use error::{Error, Result};
pub use message::{InboundMessage, OutboundMessage};
pub use paths::Paths;

/// Truncate a string at a safe UTF-8 char boundary (by char count, not byte offset).
pub fn truncate_str(s: &str, max_chars: usize) -> &str {
    if s.len() <= max_chars {
        return s;
    }
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}
