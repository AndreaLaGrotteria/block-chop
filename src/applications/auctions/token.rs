use crate::applications::auctions::Bid;

pub(in crate::applications::auctions) struct Token {
    owner: u64,
    best_bid: Option<Bid>,
}
