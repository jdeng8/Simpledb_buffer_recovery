# Simpledb_buffer_recovery

Part A: Buffer Management
Task description:
	Task 1: Use a data structure to keep track of the buffer pool
		This structure will track allocated buffers, keyed on the
		block they contain. Use this structure to determine if a 
		block is currently in a buffer. When a buffer is replaced, 
		you must update the data structure -- The mapping for the 
		old block must be removed, and the mapping for the new 
		block must be added. For our convenience, we will be using 
		“bufferPoolMap” as the name of the structure.
	Plan 1: Use a hash map to trak each block pined to a buffer.
		The key of this hash map is a concatenation of file name 
		and block number. The value is the Buffer object.
	Files changed: (with comment Task 1)
		BasicBufferMar.java

	Task 2: First In First Out (FIFO) Buffer Replacement Policy
		This suggests a page replacement strategy that chooses the 
		page that was least recently replaced i.e. the page that 
		has been sitting in the buffer pool the longest. This 
		differs a little from the least recently used in that FIFO 
		considers when the page was added to the pool while LRU 
		considers when the page was	last accessed. 
	Plan 2: Use a Linked list of Block object as a FIFO queue. 
	Files changed: (with comment Task 1)
		BasicBufferMgr.java 


Part B: Recovery Management
	Task 1: 
		The log manager needs to be modified so that it is possible 
		to read the log forward, starting from any given log record 
		(as well as from the beginning of the log). Currently, the 
		records in a log block are chained backwards. They need to 
		be chained forwards as well.
	Task 2:
		Log records need to be modified so that they contain both 
		the before and after values of the modified location.
	Task 3: 
		The method doRecover in RecoveryMgr needs to be modified 
		to support the redo stage of algorithm.

	Plan: Create a stack to record the position of each log record. 
		In undo phase, push the position of each record to the stack 
		while iterating through the log file in reverse order. In 
		redo phase, pull positions of log records from the stack 
		sequentially to read forward. Change log record content and 
		add redo algorithm.
	Files changed: (with comment Task 2)
		simpledb.log
			- LogIterator.java
			- FLogIterator.java
			- LogMgr.java
		simpledb.tx.recovery
			- LogRecord.java
			- CheckpointRecord.java
			- StartRecord.java
			- CommitRecord.java
			- RollbackRecord.java
			- SetIntRecord.java
			- SetStringRecord.java
			- LogRecordIterator.java (unchanged)
			- FLogRecordIterator.java
			- RecoveryMgr.java

Part C: Testing Buffer Replacement Policy
	Unit test file: 
		simpledb.buffer
			- BufferMgrTest.java

	How to run test: 
		Run the test file directly.

	Test procedure:
		Create a buffer pool of size 8.	 
	1. Test pin: 
		Pin 10 blocks in bufferpool. The 9th Pin should wait and fail.
		Check number of available buffers in the buffer poll. It should 
		decrease by 1 after every successful pin operation.
	2. Test unpin:
		Check number of available buffers in the buffer poll. It should 
		increase by 1 after every successful unpin operation. 

		Pin blocks to bufferpool until it is full, then pin fails. After 
		unpin some buffers, a pin operation succeeds.

	3. Test replacement:
		Scenario 1: Pin a few blocks to buffers in the bufferpool. Make 
		bufferpool has pinned buffers, unpinned buffers and empty spaces.
		Initiating another pin would fill an empty buffer. 

		Scenario 2: Pin until the bufferpool is full. Unpin buffer 2, unpin 
		buffer 3. A new block should be pinned to buffer 2, as buffer 2 is 
		in front of buffer 3 in FIFO queue. Now buffer 2 is at the end of 
		FIFO queue. Unpin the block from buffer 2 and pin another new block.
		It should be pinned to buffer 3, as buffer 3 is in front of buffer 2
		in FIFO queue.


Part D: Testing LogRecordIterator and Recovery
	Unit test file: 
	RecoveryMgrTest.java in simpledb.tx.recovery package

	How to run test: 
	Run the test file directly.

	Test procedure:
	1. Test Recovery:
		Create two blocks (blk1, blk2) and pin them to two buffers (buf1, buf2). 
		Create two recovery managers (rm1, rm2) for two transactions (txid=1, txid=2).
		For transaction 1:
		Use setInt and setString to update blk1 to new values (val1, sval1).
		Unpin buf1 and then pin blk1. Print the buffer values to check changes.
		Commit transaction 1.
		For transaction 2:
		Use setInt and setString to update blk2 to new values (val2, sval2).
		Recover all transactions, and then check two block values.
		The values for blk1 should be val1 and sval1. The values for blk2 
		should be original values.
	2. Test LogIterator:
		After the above operations, several logs for two transactions have been 
		written to log. Print log record in reverse order and forward order.

	The two tests are combined in one file, for success of Recovery test is based 
	on the success of LogRecordIterator test.
