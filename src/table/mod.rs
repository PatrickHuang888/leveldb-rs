use crate::DBError;
use crate::util;

mod block;
pub(crate) mod table;

const BL_MAX_ENCODED_LENGTH: usize = 10 + 10;
