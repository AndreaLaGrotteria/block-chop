#[derive(PartialEq, Eq)]
pub(in crate::applications::auctions) struct Bid {
    pub bidder: u64,
    pub offer: u64,
}

impl Bid {
    pub fn outbids(&self, other: &Option<Bid>) -> bool {
        match other {
            Some(other) => self.offer > other.offer,
            None => true,
        }
    }
}
