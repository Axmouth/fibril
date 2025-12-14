#[async_trait::async_trait]
pub trait Coordination: Send + Sync {
    async fn is_leader(&self, topic: &str, partition: u32) -> bool;
    async fn await_leadership(&self, topic: &str, partition: u32);
    // stub: no-op for single node now
}
pub struct NoopCoordination;

#[async_trait::async_trait]
impl Coordination for NoopCoordination {
    async fn is_leader(&self, _: &str, _: u32) -> bool {
        true
    }

    async fn await_leadership(&self, _: &str, _: u32) {
        // no-op
    }
}
