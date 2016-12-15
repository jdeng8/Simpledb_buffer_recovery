package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import simpledb.file.*;

import java.util.Iterator;
import java.util.Stack;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */
class FLogIterator implements Iterator<BasicLogRecord> {
   private Block blk;
   private Page pg = new Page();
   private int currentrec;
   private Stack<Integer> recordOffset;
   
   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    */
   FLogIterator(Block blk) {
	   //Get the stack of log record positions from LogIterator
	   recordOffset = LogIterator.getRecordOffset();
	   currentrec = recordOffset.peek();
	   //Get the current block of the last checkpoint
	   this.blk = LogIterator.getCurrentBlock();
	   pg.read(blk);
	   pg.getInt(LogMgr.LAST_POS);
	   blk.number();
	   pg.read(this.blk);	   
   }
   
   /**
    * Determines if the current log record
    * is the newest record in the log file.
    * @return true if there is a newer record
    */
   public boolean hasNext() {
	   //When there is only one record left in stack, the record is the newest one in the log file
	   return (recordOffset.size() > 1);
   }
   
   
   /**
    * Moves to the next log record in forward order.
    * If the current log record is the newest in its block,
    * then the method moves to the next newest block,
    * and returns the log record from there.
    * @return the next newest log record
    */ 
   public BasicLogRecord next() {
	   //Get the next log record position from the stack
	   currentrec = recordOffset.pop();
	   if (currentrec == 0) {
		   moveToFNextBlock();
	   }
	   return new BasicLogRecord(pg, currentrec + INT_SIZE);
   }
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
   
   /**
    * Moves to the next log block in forward order,
    * return the block number.
    */
   
   private void moveToFNextBlock() {
      blk = new Block(blk.fileName(), blk.number() + 1);
      pg.read(blk);
   }
}
