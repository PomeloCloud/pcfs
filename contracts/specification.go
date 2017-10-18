package contracts

// PCFS contracts run on BFTRaft
// Represent as functions have byte array as input and byte array as output
// It need to register to the BFTRaft before the server started
// It is not smart contract, but static
// Invoked by BFTRaft state machine ExecCommand calls

// PCFS contracts only for file meta data management
// Block: a key-value item contains arbitrary length of bytes, can be get/set/del by key
//		  block is for storing file content data, in binary format.
// 		  if the file exceeds a fixed block size, like 4kb, new block will produced
// 		  it also contain a hash field for integrity verification.
// 		  the hash will be recalculated when block data altered.
// File: meta data for files. block index, name size, timestamps etc
// Directory: indices for files. may also contain sub-directory if required
// Volume: indices for volume. unspecified, will be used for user control
// HostStash: meta data to track node storage capacity.
// 			  only the node can create and alter it's own stash data with signed client request

// The BFTRaft beta group will store all of those meta data but not those blocks binary data.
// Blocks will be addressed first from beta group by it's host id and then by key in the node
// 		  will be created by the client. it's replacement will be determined by the beta group
//		  binary file data will not go through the contracts, it only decide placement
//		  read block data will query beta nodes once and pick up the majority. this also won't go through contracts

// To write append a file the client should first try to broadcast write to hosts that contains the last block
// 		If the block size will exceed it's block size, those nodes should return remaining byte count
// 			so client can create a new block. this will keep the maximum size of blocks controlled for replication
// 		To create a new block the client will need to provide the file name and it's previous block key for verification
//		The first block of the file will be created when first open (create) this file
//		Each blocks that precedes the last block should have a fixed size (4kb) to make seek works efficiently

// Seek will not be implemented by contracts. The client will just get the file meta data and then search the
// 		appreciate block from blocks field when the block sizes are ensured

// To avoid data write conflicts, writer client should first acquire write lock for the block from the contract
//      then renew it every 1 minute to prevent it from been obsolete
// A WriteLock should contain node id, expire time, and a client signature.
// To write a block, stash server will check for existence of the WriteLock and then verify it