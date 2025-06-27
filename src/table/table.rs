use super::BlockHandle;
use super::block::Block;
use crate::Options;

struct Table<'a> {
    options: Options,

    metaindex_handle: BlockHandle,
    index_block: Block<'a>,
}
