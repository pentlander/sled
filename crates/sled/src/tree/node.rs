use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Node {
    pub id: PageID,
    pub data: Data,
    pub next: Option<PageID>,
    pub lo: Bound,
    pub hi: Bound,
}

impl Node {
    fn prefix_decode_key(&self, key: KeyRef) -> Key {
        prefix_decode(self.lo.inner(), key)
    }

    pub fn apply(
        &mut self,
        frag: &Frag,
        merge_operator: Option<usize>,
    ) {
        use self::Frag::*;

        match *frag {
            Set(ref k, ref v) => {
                let decoded_k = self.prefix_decode_key(k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    self.set_leaf(k.clone(), v.clone());
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            Merge(ref k, ref v) => {
                let decoded_k = self.prefix_decode_key(k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    let merge_fn_ptr = merge_operator
                        .expect("must have a merge operator set");
                    unsafe {
                        let merge_fn: MergeOperator =
                            std::mem::transmute(merge_fn_ptr);
                        self.merge_leaf(
                            k.clone(),
                            v.clone(),
                            merge_fn,
                        );
                    }
                } else {
                    panic!("tried to consolidate set at key <= hi")
                }
            }
            ChildSplit(ref child_split) => {
                self.child_split(child_split);
            }
            ParentSplit(ref parent_split) => {
                self.parent_split(parent_split);
            }
            Del(ref k) => {
                let decoded_k = self.prefix_decode_key(k);
                if Bound::Inclusive(decoded_k) < self.hi {
                    self.del_leaf(k);
                } else {
                    panic!("tried to consolidate del at key <= hi")
                }
            }
            Base(_, _) => {
                panic!("encountered base page in middle of chain")
            }
        }
    }

    pub fn set_leaf(&mut self, key: Key, val: Value) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(
                |&(ref k, ref _v)| prefix_cmp(k, &*key),
            );
            if let Ok(idx) = search {
                records.push((key, val));
                records.swap_remove(idx);
            } else {
                records.push((key, val));
                records.sort_unstable_by(|a, b| {
                    prefix_cmp(&*a.0, &*b.0)
                });
            }
        } else {
            panic!("tried to Set a value to an index");
        }
    }

    pub fn merge_leaf(
        &mut self,
        key: Key,
        val: Value,
        merge_fn: MergeOperator,
    ) {
        let decoded_k = self.prefix_decode_key(&key);
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(
                |&(ref k, ref _v)| prefix_cmp(k, &*key),
            );

            if let Ok(idx) = search {
                let new = merge_fn(
                    &*decoded_k,
                    Some(&records[idx].1),
                    &val,
                );
                if let Some(new) = new {
                    records.push((key, new));
                    records.swap_remove(idx);
                } else {
                    records.remove(idx);
                }
            } else {
                let new = merge_fn(&*decoded_k, None, &val);
                if let Some(new) = new {
                    records.push((key, new));
                    records.sort_unstable_by(|a, b| {
                        prefix_cmp(&*a.0, &*b.0)
                    });
                }
            }
        } else {
            panic!("tried to Merge a value to an index");
        }
    }

    pub fn child_split(&mut self, cs: &ChildSplit) {
        self.data.drop_gte(&cs.at, self.lo.inner());
        self.hi = Bound::Exclusive(cs.at.inner().to_vec());
        self.next = Some(cs.to);
    }

    pub fn parent_split(&mut self, ps: &ParentSplit) {
        if let Data::Index(ref mut ptrs) = self.data {
            let encoded_sep =
                prefix_encode(self.lo.inner(), ps.at.inner());
            ptrs.push_and_sort((encoded_sep, ps.to));
        } else {
            panic!("tried to attach a ParentSplit to a Leaf chain");
        }
    }

    pub fn del_leaf(&mut self, key: KeyRef) {
        if let Data::Leaf(ref mut records) = self.data {
            let search = records.binary_search_by(
                |&(ref k, ref _v)| prefix_cmp(k, &*key),
            );
            if let Ok(idx) = search {
                records.remove(idx);
            }
        } else {
            panic!("tried to attach a Del to an Index chain");
        }
    }

    pub fn should_split(&self, fanout: u8) -> bool {
        self.data.len() > fanout as usize
    }

    pub fn split(&self, id: PageID) -> Node {
        let (split, right_data) = self.data.split(self.lo.inner());
        Node {
            id: id,
            data: right_data,
            next: self.next,
            lo: Bound::Inclusive(split),
            hi: self.hi.clone(),
        }
    }
}
