use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

use ahash::RandomState;
use bytes::Bytes;

pub type KeyMap<V> = HashMap<Bytes, V, RandomState>;
pub type MemberSet = HashSet<Bytes, RandomState>;

#[derive(Debug, Clone)]
pub enum Value {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Hash(KeyMap<Bytes>),
    Set(MemberSet),
    ZSet(ZSetValue),
}

#[derive(Debug, Clone, Default)]
pub struct ZSetValue {
    pub scores: KeyMap<f64>,
    pub order: BTreeMap<(OrdF64, Bytes), ()>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrdF64(pub f64);

impl Eq for OrdF64 {}

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl Value {
    pub fn kind_name(&self) -> &'static str {
        match self {
            Self::Str(_) => "string",
            Self::List(_) => "list",
            Self::Hash(_) => "hash",
            Self::Set(_) => "set",
            Self::ZSet(_) => "zset",
        }
    }

    pub fn estimate_size(&self) -> u64 {
        match self {
            Self::Str(b) => b.len() as u64 + 48,
            Self::List(v) => {
                64 + v.iter().map(|b| b.len() as u64 + 32).sum::<u64>()
            }
            Self::Hash(m) => {
                64 + m.iter().map(|(k, v)| (k.len() + v.len()) as u64 + 48).sum::<u64>()
            }
            Self::Set(s) => 64 + s.iter().map(|m| m.len() as u64 + 32).sum::<u64>(),
            Self::ZSet(z) => {
                96 + z.scores.iter().map(|(m, _)| m.len() as u64 + 40).sum::<u64>()
            }
        }
    }

    pub fn is_empty_collection(&self) -> bool {
        match self {
            Self::List(v) => v.is_empty(),
            Self::Hash(m) => m.is_empty(),
            Self::Set(s) => s.is_empty(),
            Self::ZSet(z) => z.scores.is_empty(),
            Self::Str(_) => false,
        }
    }
}

pub fn parse_i64_bytes(b: &Bytes) -> Option<i64> {
    let s = std::str::from_utf8(b).ok()?;
    s.parse().ok()
}

pub fn parse_f64_bytes(b: &Bytes) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?;
    let v: f64 = s.parse().ok()?;
    if v.is_nan() || v.is_infinite() {
        None
    } else {
        Some(v)
    }
}

pub fn validate_score(score: f64) -> bool {
    !score.is_nan()
}