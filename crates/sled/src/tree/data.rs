use std::fmt::Debug;

use super::*;

pub type KV<T> = (Key, T);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Data {
    Index(Pointers<PageID>),
    Leaf(Vec<(Key, Value)>),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pointers<T> {
    ptrs: Vec<KV<T>>,
}

impl<T> Pointers<T>
where
    T: Clone + Ord,
{
    pub fn len(&self) -> usize {
        self.ptrs.len()
    }

    pub fn get(&self, idx: usize) -> Option<&KV<T>> {
        self.ptrs.get(idx)
    }

    pub fn push_and_sort(&mut self, key_value: KV<T>) {
        self.ptrs.push(key_value);
        self.ptrs.sort_unstable_by(|a, b| prefix_cmp(&a.0, &b.0));
    }

    pub fn search(
        &self,
        encoded_key: KeyRef,
    ) -> Result<usize, usize> {
        self.ptrs.binary_search_by(|(key, _value)| {
            prefix_cmp(key, encoded_key)
        })
    }

    fn split(&self, lhs_prefix: &[u8]) -> (Key, Self) {
        let mut decoded_xs: Vec<_> = self
            .ptrs
            .iter()
            .map(|&(ref k, ref v)| {
                let decoded_k = prefix_decode(lhs_prefix, &k);
                (decoded_k, v.clone())
            })
            .collect();
        decoded_xs.sort();

        let (_lhs, rhs) =
            decoded_xs.split_at(decoded_xs.len() / 2 + 1);
        let split = rhs
            .first()
            .expect("rhs should contain at least one element")
            .0
            .clone();
        let rhs_data: Vec<_> = rhs
            .iter()
            .map(|&(ref k, ref v)| {
                let new_k = prefix_encode(&split, k);
                (new_k, v.clone())
            })
            .collect();

        (split, Pointers { ptrs: rhs_data })
    }
}

impl Data {
    pub fn index(index_vec: Vec<KV<PageID>>) -> Data {
        Data::Index(Pointers { ptrs: index_vec })
    }

    pub fn len(&self) -> usize {
        match *self {
            Data::Index(ref ptrs) => ptrs.len(),
            Data::Leaf(ref items) => items.len(),
        }
    }

    pub fn split(&self, lhs_prefix: &[u8]) -> (Key, Data) {
        fn split_inner<T>(
            xs: &[(Key, T)],
            lhs_prefix: &[u8],
        ) -> (Key, Vec<(Key, T)>)
        where
            T: Clone + Debug + Ord,
        {
            let mut decoded_xs: Vec<_> = xs
                .iter()
                .map(|&(ref k, ref v)| {
                    let decoded_k = prefix_decode(lhs_prefix, &*k);
                    (decoded_k, v.clone())
                })
                .collect();
            decoded_xs.sort();

            let (_lhs, rhs) =
                decoded_xs.split_at(decoded_xs.len() / 2 + 1);
            let split = rhs
                .first()
                .expect("rhs should contain at least one element")
                .0
                .clone();
            let rhs_data: Vec<_> = rhs
                .iter()
                .map(|&(ref k, ref v)| {
                    let new_k = prefix_encode(&*split, k);
                    (new_k, v.clone())
                })
                .collect();

            (split, rhs_data)
        }

        match *self {
            Data::Index(ref ptrs) => {
                let (split, rhs) = ptrs.split(lhs_prefix);
                (split, Data::Index(rhs))
            }
            Data::Leaf(ref items) => {
                let (split, rhs) = split_inner(items, lhs_prefix);
                (split, Data::Leaf(rhs))
            }
        }
    }

    pub fn drop_gte(&mut self, at: &Bound, prefix: &[u8]) {
        let bound = at.inner();
        match *self {
            Data::Index(ref mut ptrs) => {
                ptrs.ptrs.retain(|&(ref k, _)| {
                    let decoded_k = prefix_decode(prefix, &*k);
                    &*decoded_k < bound
                })
            }
            Data::Leaf(ref mut items) => {
                items.retain(|&(ref k, _)| {
                    let decoded_k = prefix_decode(prefix, &*k);
                    &*decoded_k < bound
                })
            }
        }
    }

    pub fn leaf(&self) -> Option<Vec<(Key, Value)>> {
        match *self {
            Data::Index(_) => None,
            Data::Leaf(ref items) => Some(items.clone()),
        }
    }

    pub fn leaf_ref(&self) -> Option<&Vec<(Key, Value)>> {
        match *self {
            Data::Index(_) => None,
            Data::Leaf(ref items) => Some(items),
        }
    }
}
