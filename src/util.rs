use num_traits::PrimInt;
use std::str::FromStr;

pub(crate) fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    let int = int_str.parse::<T>();
    match int {
        Ok(i) if !i.is_zero() => Some(i),
        _ => None
    }
}

pub(crate) fn digit_vec(int_str: &str) -> Vec<u8> {
    int_str
        .chars()
        .filter_map(|c|c.to_digit(10))
        .map(|u| u as u8)
        .collect()
}
