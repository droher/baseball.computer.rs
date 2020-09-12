use num_traits::PrimInt;
use std::str::FromStr;
use anyhow::{Error, Result, anyhow, Context};
use std::convert::TryFrom;

pub(crate) fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    let int = int_str.parse::<T>();
    match int {
        Ok(i) if !i.is_zero() => Some(i),
        _ => None
    }
}
