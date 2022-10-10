use crate::applications::auctions::Bid;

pub(in crate::applications::auctions) struct Token {
    pub owner: u64,
    pub best_bid: Option<Bid>,
}
