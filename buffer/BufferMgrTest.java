package simpledb.buffer;

import simpledb.server.*;
import simpledb.buffer.*;
import simpledb.file.*;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class BufferMgrTest {

	@Before
	public void setUp() throws Exception {
		 SimpleDB.init("simpleDB");
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPin() {
		 System.out.println("testPin start------------------------!");

		 Block[] blk1=new Block[10];
		 for(int i=0;i<10;i++){
			 blk1[i] = new Block("filename", i);
		 }
		 BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		 //initially, available buffers should be 8
		 assertEquals(8, basicBufferMgr.available());

		 for(int i=0;i<10;i++){
			 
			//pin a block to buffer,if buffer is full, it will wait for some time then fail
			try {
			     basicBufferMgr.pin(blk1[i]); //pin a block to buffer
				 assertEquals(8-i-1, basicBufferMgr.available());	
			     }
			catch (BufferAbortException e) {
				 System.out.println(i+" buffer pin fails!");//buffer pool is full
			} 
		 }
	     
		 System.out.println("testPin end！-------------------------");
		 blk1=null;
	}

	@Test
	public void testUnpin() {
		 System.out.println("testUnpin start------------------------!");
		 
		 //initiallize 10 blocks
		 Block[] blk=new Block[10];
		 Buffer[] buf=new Buffer[10];
		 for(int i=0;i<10;i++){
			 blk[i] = new Block("filename", i);
		 }
		 BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		 //initially, available buffers should be 8
		 assertEquals(8, basicBufferMgr.available());
		 
		 //pin
		 for(int i=0;i<9;i++){
				try {
				     buf[i]=basicBufferMgr.pin(blk[i]); //pin a block to buffer
				     }
				catch (BufferAbortException e) {
					 System.out.println(i+" buffer pin fails!");//buffer pool is full
				}
		 }
		 assertEquals(0, basicBufferMgr.available());//since pool is full, available num is 0

		 //unpin
		 for(int i=0;i<2;i++){

			 //before unpin, available buffers should be i
			 assertEquals(i, basicBufferMgr.available());
				try {
				     basicBufferMgr.unpin(buf[i]); //unpin a buffer
				     }
				catch (BufferAbortException e) {
					 System.out.println(i+" buffer unpin fails!");
				}
			 //after pin, the available buffer is increased by 1
			 assertEquals(i+1, basicBufferMgr.available());
		 }
		 
		 //after unpin, it can pin again!
			try {
			     basicBufferMgr.pin(blk[9]); //pin a block to buffer
			     }
			catch (BufferAbortException e) {
				 System.out.println("buffer pin fails after unpin!");
			}
			assertEquals(1, basicBufferMgr.available());
		 System.out.println("testUnpin end！-------------------------");
		 blk=null;
		 buf=null;
	}
	
	@Test
	public void testReplacement() {
		 System.out.println("testreplacement start！-------------------------");
		 
		 Block[] blk=new Block[12];
		 Buffer[] buf=new Buffer[12];
	     Buffer [] bufpool=new Buffer[8];
	     
	     //initiallize blocks
		 for(int i=0;i<12;i++){
			 blk[i] = new Block("filename", i);
		 }
		 BufferMgr basicBufferMgr = new SimpleDB().bufferMgr();
		 
		 basicBufferMgr.clearBufferPool();

		 //pin 6 blocks to buffer and keep the buffer not full
		 for(int i=0;i<6;i++){
				try {
				     buf[i]=basicBufferMgr.pin(blk[i]); //pin a block to buffer
				     bufpool=basicBufferMgr.pool();
				     //assert the block is pinned to buffer pool sequentially before buffer pool is full
				     assertEquals(buf[i],bufpool[i]);	
				     }
				catch (BufferAbortException e) {
					 System.out.println(i+" buffer pin fails!");
				}
		 }

		 //unpin 2,3 buffers
		 for(int i=2;i<4;i++){
				try {
				     basicBufferMgr.unpin(buf[i]); //unpin a buffer
				     bufpool=basicBufferMgr.pool();
				     //assert the unpinned buffer is 2 and 3
				     assertEquals(false,bufpool[i].isPinned());
				     }
				catch (BufferAbortException e) {
					 System.out.println(i+" buffer unpin fails!");
				}
		 }

		 //pin when buffer is not full, it should first pin the empty buffer
		try {
		     buf[6]=basicBufferMgr.pin(blk[6]); //pin block 9 to a buffer
		     bufpool=basicBufferMgr.pool();
		     assertEquals(buf[6],bufpool[6]);//assert the newly pinned buffer was the empty buffer in buffer pool
		     }
		catch (BufferAbortException e) {
			 System.out.println("buffer pin fails!");
		}
		 try {
		     buf[7]=basicBufferMgr.pin(blk[7]); //pin block 9 to a buffer
		     }
		catch (BufferAbortException e) {
			 System.out.println("buffer pin fails!");
		}

		//pin buffer 2, it should be move to last position in replacement queue
		//unpin 2, 2 and 3 are both unpinned, a new block should be pinned at 3.

		try {
		     buf[9]=basicBufferMgr.pin(blk[9]); 
		     assertEquals(buf[9],bufpool[2]);
		     }
		catch (BufferAbortException e) {
			 System.out.println("buffer 9 pin fails");
		}

		try {
		     basicBufferMgr.unpin(buf[9]); 
		     }
		catch (BufferAbortException e) {
			 System.out.println("block 9 unpin fails");
		}

		try {
		     buf[11]=basicBufferMgr.pin(blk[11]); //pin a block to buffer
		     assertEquals(buf[11],bufpool[3]);
		     }
		catch (BufferAbortException e) {
			 System.out.println("buffer 11 pin fails");
		}

	     System.out.println("testreplacement end！-------------------------");
		
	}


}
