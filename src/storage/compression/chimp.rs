use std::io::{self};

pub struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    nbits: u8,
}

impl BitWriter {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            cur: 0,
            nbits: 0,
        }
    }
    pub fn write_bit(&mut self, bit: bool) {
        self.cur <<= 1;
        if bit {
            self.cur |= 1;
        }
        self.nbits += 1;
        if self.nbits == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.nbits = 0;
        }
    }
    pub fn write_u64_bits(&mut self, x: u64) {
        for i in (0..64).rev() {
            let b = ((x >> i) & 1) != 0;
            self.write_bit(b);
        }
    }
    pub fn finish(mut self) -> Vec<u8> {
        if self.nbits != 0 {
            self.cur <<= 8 - self.nbits;
            self.buf.push(self.cur);
        }
        self.buf
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BitReader<'a> {
    buf: &'a [u8],
    idx: usize,
    cur: u8,
    nbits: u8,
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        let mut r = Self {
            buf,
            idx: 0,
            cur: 0,
            nbits: 0,
        };
        r.refresh();
        r
    }
    fn refresh(&mut self) {
        if self.nbits == 0 && self.idx < self.buf.len() {
            self.cur = self.buf[self.idx];
            self.idx += 1;
            self.nbits = 8;
        }
    }
    pub fn read_bit(&mut self) -> io::Result<bool> {
        if self.nbits == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "no more bits"));
        }
        let b = (self.cur & 0x80) != 0;
        self.cur <<= 1;
        self.nbits -= 1;
        if self.nbits == 0 {
            self.refresh();
        }
        Ok(b)
    }
    pub fn read_u64_bits(&mut self) -> io::Result<u64> {
        let mut x = 0u64;
        for _ in 0..64 {
            x <<= 1;
            x |= if self.read_bit()? { 1 } else { 0 };
        }
        Ok(x)
    }
}

/// Naive XOR-based float compressor (experimental):
/// First value stored fully (64 bits). For next values: write 0 bit if same,
/// else write 1 bit and the full 64-bit value. This is a simplified baseline
/// for future Chimp-style optimizations.
pub fn encode_series(vals: &[f64]) -> Vec<u8> {
    let mut w = BitWriter::new();
    // Write count header (u64) to avoid EOF/padding ambiguity
    w.write_u64_bits(vals.len() as u64);
    let mut prev_bits: u64 = 0;
    for (i, v) in vals.iter().enumerate() {
        let bits = v.to_bits();
        if i == 0 {
            w.write_bit(true);
            w.write_u64_bits(bits);
        } else {
            let xor = bits ^ prev_bits;
            if xor == 0 {
                w.write_bit(false);
            } else {
                w.write_bit(true);
                w.write_u64_bits(bits);
            }
        }
        prev_bits = bits;
    }
    w.finish()
}

pub fn decode_series(buf: &[u8]) -> io::Result<Vec<f64>> {
    let mut r = BitReader::new(buf);
    let count = r.read_u64_bits()? as usize;
    let mut out = Vec::with_capacity(count);
    let mut prev_bits: u64 = 0;
    for i in 0..count {
        let bit = r.read_bit()?;
        let bits = if i == 0 {
            if !bit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "first value must be full",
                ));
            }
            let b = r.read_u64_bits()?;
            prev_bits = b;
            b
        } else if !bit {
            prev_bits
        } else {
            let b = r.read_u64_bits()?;
            prev_bits = b;
            b
        };
        out.push(f64::from_bits(bits));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_simple_series() {
        let vals = vec![
            1.0,
            1.0,
            2.5,
            2.5,
            std::f64::consts::PI,
            std::f64::consts::PI,
            -42.0,
        ];
        let enc = encode_series(&vals);
        let dec = decode_series(&enc).expect("decode");
        assert_eq!(vals.len(), dec.len());
        for (a, b) in vals.iter().zip(dec.iter()) {
            assert!((a - b).abs() < 1e-12);
        }
    }
}
