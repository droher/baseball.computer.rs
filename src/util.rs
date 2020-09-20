use num_traits::PrimInt;
use std::str::FromStr;
use std::ops::Deref;
use anyhow::{anyhow, Result};

pub(crate) fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    int_str
        .parse::<T>()
        .ok()
        .filter(|i| !i.is_zero())
}

pub(crate) fn digit_vec(int_str: &str) -> Vec<u8> {
    int_str
        .chars()
        .filter_map(|c|c.to_digit(10))
        .map(|u| u as u8)
        .collect()
}

pub(crate) fn pop_plus_vec(mut vec: Vec<u8>) -> (Option<u8>, Vec<u8>) {
    (vec.pop(), vec)
}

pub(crate) fn str_to_tinystr<T: FromStr>(s: &str) -> Result<T> {
    T::from_str(s).map_err({|_| anyhow!("Tinystr not formatted properly")})
}

