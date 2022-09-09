use async_trait::async_trait;

#[async_trait]
pub trait Order: 'static + Send + Sync {
    async fn order(&self, payload: &[u8]);
    async fn deliver(&self) -> Vec<u8>;
}
