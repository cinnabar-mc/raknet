// Modified by Cinnabar contributors for explicit connection endpoints and wire address bounds.
use crate::protocol::codec::RakCodec;
use crate::protocol::error::RakCodecError;
use crate::util::constants::SYSTEM_ADDRESS_COUNT;
use crate::util::socket_addr::canonical_socket_addr;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

impl RakCodec for SocketAddr {
    fn serialize<W: Write>(&self, writer: &mut W) -> Result<(), RakCodecError> {
        match canonical_socket_addr(*self) {
            SocketAddr::V4(addr) => {
                writer.write_u8(4)?;
                writer.write_all(&addr.ip().octets().map(|octet| !octet))?;
                writer.write_u16::<BigEndian>(addr.port())?;
            }
            SocketAddr::V6(addr) => {
                writer.write_u8(6)?;
                writer.write_u16::<LittleEndian>(23)?;
                writer.write_u16::<BigEndian>(addr.port())?;
                writer.write_u32::<BigEndian>(0)?;
                writer.write_all(&addr.ip().octets())?;
                writer.write_u32::<BigEndian>(0)?;
            }
        }

        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> Result<Self, RakCodecError> {
        match reader.read_u8()? {
            0 | 4 => {
                let mut octets = [0u8; 4];
                reader.read_exact(&mut octets)?;
                let ip = Ipv4Addr::from(octets.map(|octet| !octet));
                let port = reader.read_u16::<BigEndian>()?;

                Ok(SocketAddr::V4(SocketAddrV4::new(ip, port)))
            }
            6 => {
                reader.read_u16::<LittleEndian>()?;
                let port = reader.read_u16::<BigEndian>()?;
                reader.read_u32::<BigEndian>()?;
                let mut octets = [0u8; 16];
                reader.read_exact(&mut octets)?;
                let ip = Ipv6Addr::from(octets);
                reader.read_u32::<BigEndian>()?;

                Ok(canonical_socket_addr(SocketAddr::V6(SocketAddrV6::new(
                    ip, port, 0, 0,
                ))))
            }
            _ => Err(RakCodecError::Malformed("socket addr")),
        }
    }

    fn size_hint(&self) -> usize {
        size_of::<u8>()
            + match canonical_socket_addr(*self) {
                SocketAddr::V4(..) => 4 + size_of::<u16>(),
                SocketAddr::V6(..) => {
                    size_of::<u16>() + size_of::<u16>() + size_of::<u32>() + 16 + size_of::<u32>()
                }
            }
    }
}

/// Reads at most twenty wire addresses and their two trailing timestamps.
pub(crate) fn read_system_addresses_and_timestamps<R: Read>(
    reader: &mut R,
) -> Result<(Vec<SocketAddr>, u64, u64), RakCodecError> {
    const MAX_BODY_SIZE: usize =
        SYSTEM_ADDRESS_COUNT * (1 + 2 + 2 + 4 + 16 + 4) + 2 * size_of::<u64>();
    let mut bytes = Vec::new();
    reader
        .take((MAX_BODY_SIZE + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_BODY_SIZE {
        return Err(RakCodecError::Malformed("system address body is too large"));
    }
    let mut reader = Cursor::new(bytes.as_slice());
    let mut addresses = Vec::new();
    while bytes.len() - reader.position() as usize > 2 * size_of::<u64>() {
        if addresses.len() == SYSTEM_ADDRESS_COUNT {
            return Err(RakCodecError::Malformed("too many system addresses"));
        }
        addresses.push(SocketAddr::deserialize(&mut reader)?);
    }
    let first_timestamp = reader.read_u64::<BigEndian>()?;
    let second_timestamp = reader.read_u64::<BigEndian>()?;
    Ok((addresses, first_timestamp, second_timestamp))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn round_trip(addr: SocketAddr, expected: &[u8]) {
        let mut encoded = Vec::new();
        addr.serialize(&mut encoded).unwrap();
        assert_eq!(encoded, expected, "encoding differs from RakNet");
        assert_eq!(encoded.len(), addr.size_hint());
        assert_eq!(
            SocketAddr::deserialize(&mut Cursor::new(expected)).unwrap(),
            addr
        );
    }

    #[test]
    fn ipv4_octets_are_bit_inverted() {
        round_trip(
            "127.0.0.1:19132".parse().unwrap(),
            &[0x04, 0x80, 0xff, 0xff, 0xfe, 0x4a, 0xbc],
        );
    }

    #[test]
    fn ipv6_family_is_little_endian() {
        let mut expected = vec![0x06, 0x17, 0x00, 0x4a, 0xbc, 0, 0, 0, 0];
        expected.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        expected.extend_from_slice(&[0, 0, 0, 0]);
        round_trip("[::1]:19132".parse().unwrap(), &expected);
    }
}
