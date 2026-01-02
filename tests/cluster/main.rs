mod test_cluster;

use example_raft_key_value::store::ExampleStore;
use example_raft_key_value::ExampleNodeId;
use openraft::testing::Suite;
use openraft::StorageError;
use std::sync::Arc;

pub async fn new_async() -> Arc<ExampleStore> {
    let res = ExampleStore::open_create(0);

    Arc::new(res)
}

#[test]
pub fn test_mem_store() -> Result<(), StorageError<ExampleNodeId>> {
    Suite::test_all(new_async)?;
    Ok(())
}
