use num_traits::PrimInt;
use std::str::FromStr;
use smallvec::SmallVec;

pub(crate) fn parse_positive_int<T: PrimInt + FromStr>(int_str: &str) -> Option<T> {
    int_str
        .parse::<T>()
        .ok()
        .filter(|i| !i.is_zero())
}

pub(crate) fn digit_vec(int_str: &str) -> SmallVec<[u8; 3]> {
    int_str
        .chars()
        .filter_map(|c|c.to_digit(10))
        .map(|u| u as u8)
        .collect()
}

pub(crate) fn pop_plus_vec(mut vec: SmallVec<[u8; 3]>) -> (Option<u8>, SmallVec<[u8; 3]>) {
    (vec.pop(), vec)
}