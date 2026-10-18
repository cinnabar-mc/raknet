//! Client GUID values and explicit operating-system entropy generation. See NOTICE.

use rand::{TryRng, rngs::SysRng};
use std::error::Error;
use std::io;

const PAYLOAD_MASK: u64 = (1_u64 << 62) - 1;
const CLIENT_TYPE_BITS: u64 = 1_u64 << 63;

/// The client-supplied identity for one RakNet connection or ping.
///
/// Generated values use fresh OS entropy with their top two bits set to `10`.
/// Explicit wire values remain available for custom peers and deterministic fixtures.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientGuid(u64);

impl ClientGuid {
    /// Generates one identity from eight fresh operating-system entropy bytes.
    ///
    /// # Errors
    /// Returns the entropy failure, without falling back to cached or weak randomness.
    pub fn generate() -> io::Result<Self> {
        Self::from_rng(&mut SysRng)
    }

    /// Uses an explicit wire value, preserving custom protocol identities unchanged.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Returns the identity's wire representation.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Reads and formats one complete entropy value, propagating a source failure.
    fn from_rng<R: TryRng>(rng: &mut R) -> io::Result<Self>
    where
        R::Error: Error + Send + Sync + 'static,
    {
        let mut bytes = [0; size_of::<u64>()];
        rng.try_fill_bytes(&mut bytes).map_err(io::Error::other)?;
        Ok(Self(
            u64::from_le_bytes(bytes) & PAYLOAD_MASK | CLIENT_TYPE_BITS,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    struct Entropy {
        bytes: VecDeque<u8>,
        reads: usize,
    }

    impl TryRng for Entropy {
        type Error = io::Error;

        fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
            Err(io::Error::other("identity generation must request bytes"))
        }

        fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
            Err(io::Error::other("identity generation must request bytes"))
        }

        fn try_fill_bytes(&mut self, destination: &mut [u8]) -> Result<(), Self::Error> {
            assert_eq!(destination.len(), size_of::<u64>());
            self.reads += 1;
            for byte in destination {
                *byte = self
                    .bytes
                    .pop_front()
                    .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
            }
            Ok(())
        }
    }

    #[test]
    fn each_generated_identity_reads_fresh_entropy_sets_type_bits_and_propagates_failure() {
        let mut bytes = 0x1122334455667788_u64.to_le_bytes().to_vec();
        bytes.extend_from_slice(&0xffeeddccbbaa0099_u64.to_le_bytes());
        let mut entropy = Entropy {
            bytes: bytes.into(),
            reads: 0,
        };
        let first = ClientGuid::from_rng(&mut entropy).unwrap();
        let second = ClientGuid::from_rng(&mut entropy).unwrap();
        assert_eq!(first.as_u64(), 0x9122334455667788);
        assert_eq!(second.as_u64(), 0xbfeeddccbbaa0099);
        assert_ne!(first, second);
        let error = ClientGuid::from_rng(&mut entropy).unwrap_err();
        assert_eq!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .unwrap()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
        assert_eq!(entropy.reads, 3);
    }
}
