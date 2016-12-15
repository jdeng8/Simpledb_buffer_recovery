package simpledb.tx.recovery;

import simpledb.server.*;
import simpledb.buffer.*;
import simpledb.file.*;
import simpledb.log.*;
import simpledb.tx.*;

import static org.junit.Assert.*;
import static simpledb.tx.recovery.LogRecord.START;

import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RecoveryMgrTest {

	@Test
	public void testRecover() {
		SimpleDB.init("simpleDB");
		
		System.out.println("Test recovery--------------------------");
		
		// Create new blocks and new buffers.
		Block blk1 = new Block("testfile", 1);
		Block blk2 = new Block("testfile", 2);
		Buffer buf1 = new Buffer();
		Buffer buf2 = new Buffer();
		int oldint1 = 0, oldint2 = 0;
		String oldstr1 = "", oldstr2 = "";
		// Create two recovery managers with transaction number 1 and 2.
		RecoveryMgr rm1 = new RecoveryMgr(1);
		RecoveryMgr rm2 = new RecoveryMgr(2);
		
		Transaction tx = new Transaction();
		
		BufferMgr bm = new SimpleDB().bufferMgr();
		try {
			 //Pin a block to buffer
		     buf1 = bm.pin(blk1); 
		     buf2 = bm.pin(blk2);
		     //Store the old values
		     oldint1 = buf1.getInt(0);
		     oldstr1 = buf1.getString(4);
		     oldint2 = buf2.getInt(0);
		     oldstr2 = buf2.getString(4);
		}
		catch (BufferAbortException e) {
			 System.out.println("buffer pin fails!");
		}
		
		// Make updates to the block and write logs 
		System.out.println("Modify the block2 in transaction 1 and 2:");
		
		int val1 = 1;
		String sval1 = "hello";
		int val2 = 10;
		String sval2 = "world";
		
		//tx1 update operations
		System.out.println("Tx1: set int to " + val1 + " for blk1");
		int lsn = rm1.setInt(buf1, 0, val1);
		buf1.setInt(0, val1, 1, lsn);
		System.out.println("Tx1: set string to " + sval1 + " for blk1");
		lsn = rm1.setString(buf1, 4, sval1);
		buf1.setString(4, sval1, 1, lsn);
		//Unpin and pin the block to see changes
		try {
			System.out.println("Unpin buf");
			bm.unpin(buf1);
			System.out.println("Pin blk");
		    buf1 = bm.pin(blk1);
		} catch (BufferAbortException e) {
			 System.out.println("buffer pin fails!");
		}
	    //The values should be the same as the updates: val1 and sval1
	    assertEquals(val1, buf1.getInt(0));
		assertEquals(sval1, buf1.getString(4));
		System.out.println("Values in buf1: " + buf1.getInt(0) + " " + buf1.getString(4));
		//tx1 commit
		System.out.println("Tx1 commit");
		rm1.commit();
		
		//tx2 update operations
		System.out.println("Tx2: set int to " + val2 + " for blk2");
		lsn = rm2.setInt(buf2, 0, val2);
		buf2.setInt(0, val2, 2, lsn);
		System.out.println("Tx2: set string to " + sval2 + " for blk2");
		lsn = rm2.setString(buf2, 4, sval2);
		buf2.setString(4, sval2, 2, lsn);
		
		//Recover all transactions
		System.out.println("Do recovery");
		tx.recover();
		//After recovery, blk1 values should be the new values updated in tx1
		//blk2 values should be set to old values
		assertEquals(val1, buf1.getInt(0));
		assertEquals(sval1, buf1.getString(4));
		assertEquals(oldint2, buf2.getInt(0));
		assertEquals(oldstr2, buf2.getString(4));
		System.out.println("Values in buf1: " + buf1.getInt(0) + " " + buf1.getString(4));
		System.out.println("Values in buf2: " + buf2.getInt(0) + " " + buf2.getString(4));
		
		//Print logs backwards and forwards
		System.out.println("Test LogIterator-----------------------------");
		System.out.println("Print log backwards");

	    Iterator<LogRecord> iter1 = new LogRecordIterator();
	      while (iter1.hasNext()) {
	          LogRecord rec = iter1.next();
	             
	     		System.out.println(rec);
	     		if (rec.op() == START)
	                break;
	    }
	    
	    System.out.println("Print log forwards");
	    Iterator<LogRecord> fiter1 = new FLogRecordIterator();
	    	while (fiter1.hasNext()) {
	        LogRecord rec = fiter1.next();
	     	System.out.println(rec);
	    }
	}
}
