// Modified by Cinnabar contributors for explicit connection endpoints and wire address bounds.
use crate::protocol::codec::RakCodec;
use crate::protocol::error::RakCodecError;
use crate::protocol::types::socket_addr::read_system_addresses_and_timestamps;
use crate::util::constants::SYSTEM_ADDRESS_COUNT;
use crate::util::packet_id::CONNECTION_REQUEST_ACCEPTED;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::net::SocketAddr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionRequestAccepted {
    pub client_address: SocketAddr,
    pub system_index: u16,
    pub system_addresses: Vec<SocketAddr>,
    pub request_timestamp: u64,
    pub timestamp: u64,
}

impl RakCodec for ConnectionRequestAccepted {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), RakCodecError> {
        if self.system_addresses.len() > SYSTEM_ADDRESS_COUNT {
            return Err(RakCodecError::Malformed("too many system addresses"));
        }
        writer.write_u8(CONNECTION_REQUEST_ACCEPTED)?;
        SocketAddr::serialize(&self.client_address, writer)?;
        writer.write_u16::<BigEndian>(self.system_index)?;
        for addr in &self.system_addresses {
            SocketAddr::serialize(addr, writer)?;
        }
        writer.write_u64::<BigEndian>(self.request_timestamp)?;
        writer.write_u64::<BigEndian>(self.timestamp)?;

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, RakCodecError> {
        let id = reader.read_u8()?;
        if id != CONNECTION_REQUEST_ACCEPTED {
            return Err(RakCodecError::UnexpectedPacketID(
                CONNECTION_REQUEST_ACCEPTED,
                id,
            ));
        }

        let client_address = SocketAddr::deserialize(reader)?;
        let system_index = reader.read_u16::<BigEndian>()?;

        let (system_addresses, request_timestamp, timestamp) =
            read_system_addresses_and_timestamps(reader)?;

        Ok(Self {
            client_address,
            system_index,
            system_addresses,
            request_timestamp,
            timestamp,
        })
    }

    fn size_hint(&self) -> usize {
        size_of::<u8>()
            + SocketAddr::size_hint(&self.client_address)
            + size_of::<u16>()
            + self
                .system_addresses
                .iter()
                .fold(0, |acc, addr| acc + SocketAddr::size_hint(addr))
            + size_of::<u64>()
            + size_of::<u64>()
    }
}
