use num_traits::PrimInt;
use std::str::FromStr;
use anyhow::{anyhow, Result};
use regex::{Regex, Match};

#[inline]
pub(crate) fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    int_str
        .parse::<T>()
        .ok()
        .filter(|i| !i.is_zero())
}

#[inline]
pub(crate) fn digit_vec(int_str: &str) -> Vec<u8> {
    int_str
        .chars()
        .filter_map(|c|c.to_digit(10))
        .map(|u| u as u8)
        .collect()
}

#[inline]
pub(crate) fn str_to_tinystr<T: FromStr>(s: &str) -> Result<T> {
    T::from_str(s).map_err(|_| anyhow!("Tinystr not formatted properly"))
}

#[inline]
pub(crate) fn pop_with_vec<T: Sized>(mut v: Vec<T>) -> (Option<T>, Vec<T>) {
    (v.pop(), v)
}

#[inline]
pub(crate) fn regex_split<'a>(s: &'a str, re: &'static Regex) -> (&'a str, Option<&'a str>) {
    match re.find(s) {
        None => (s, None),
        Some(m) => (&s[..m.start()], Some(&s[m.start()..]))
    }
}

#[inline]
pub(crate) fn to_str_vec(match_vec: Vec<Option<Match>>) -> Vec<&str> {
    match_vec.into_iter()
        .filter_map(|o| o.map(|m| m.as_str()))
        .collect()
}

#[inline]
pub(crate) fn count_occurrences<T: Eq>(match_vec: &Vec<T>, object: &T) -> u8 {
    match_vec.into_iter()
        .filter(|t| *t == object)
        .count() as u8
}

#[inline]
pub(crate) fn opt_add(o: &mut Option<u8>, add: u8) {
    *o = Some(o.unwrap_or_default() + add)
}


#[inline]
pub(crate) fn u8_vec_to_string(vec: Vec<u8>) -> Vec<String> {
    vec.iter().map(|u| u.to_string()).collect()
}
