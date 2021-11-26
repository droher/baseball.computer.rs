use anyhow::{anyhow, Result};
use num_traits::PrimInt;
use regex::{Match, Regex};
use std::str::FromStr;

#[inline]
pub fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    int_str.parse::<T>().ok().filter(|i| !i.is_zero())
}

#[inline]
pub fn digit_vec(int_str: &str) -> Vec<u8> {
    int_str
        .chars()
        .filter_map(|c| c.to_digit(10))
        .map(|u| u.try_into().unwrap())
        .collect()
}

#[inline]
pub fn str_to_tinystr<T: FromStr>(s: &str) -> Result<T> {
    T::from_str(s).map_err(|_| anyhow!("Tinystr not formatted properly"))
}

#[inline]
pub fn regex_split<'a>(s: &'a str, re: &'static Regex) -> (&'a str, Option<&'a str>) {
    match re.find(s) {
        None => (s, None),
        Some(m) => (&s[..m.start()], Some(&s[m.start()..])),
    }
}

#[inline]
pub fn to_str_vec(match_vec: Vec<Option<Match>>) -> Vec<&str> {
    match_vec
        .into_iter()
        .filter_map(|o| o.map(|m| m.as_str()))
        .collect()
}
