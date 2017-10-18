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
// HostStash: meta data to track node storage capacity
// The BFTRaft group will store all of those meta data but not those blocks binary data.
// Blocks will be addressed first by it's host id and then by key