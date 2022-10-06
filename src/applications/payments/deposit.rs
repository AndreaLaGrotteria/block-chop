#[derive(Clone)]
pub(in crate::applications::payments) struct Deposit {
    pub to: u64,
    pub amount: u64,
}
