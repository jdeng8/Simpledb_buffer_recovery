package simpledb.log;

import static simpledb.file.Page.INT_SIZE;
import simpledb.file.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

/**
 * A class that provides the ability to move through the
 * records of the log file in reverse order.
 * 
 * @author Edward Sciore
 */
class LogIterator implements Iterator<BasicLogRecord> {
	private Page pg = new Page();
	private int currentrec;
    //Task 2
	private static Block blk;
	//Use a stack to store the record positions for every log
	private static Stack<Integer> recordOffset = new Stack<Integer>();
   
   
   /**
    * Creates an iterator for the records in the log file,
    * positioned after the last log record.
    * This constructor is called exclusively by
    * {@link LogMgr#iterator()}.
    */
   LogIterator(Block blk) {
      this.blk = blk;
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
      //Task 2
      recordOffset.clear();
      recordOffset.push(currentrec);
   }
   
   /**
    * Determines if the current log record
    * is the earliest record in the log file.
    * @return true if there is an earlier record
    */
   public boolean hasNext() {
      return currentrec>0 || blk.number()>0;
   }
   

   /**
    * Moves to the next log record in reverse order.
    * If the current log record is the earliest in its block,
    * then the method moves to the next oldest block,
    * and returns the log record from there.
    * @return the next earliest log record
    */
   public BasicLogRecord next() {
      if (currentrec == 0) 
         moveToNextBlock();
      currentrec = pg.getInt(currentrec);
      //Task 2
      //Push every log record position in the stack
      recordOffset.push(currentrec);
      return new BasicLogRecord(pg, currentrec+INT_SIZE);
   }
   
   
   public void remove() {
      throw new UnsupportedOperationException();
   }
   
   /**
    * Moves to the next log block in reverse order,
    * and positions it after the last record in that block.
    */
   private void moveToNextBlock() {
      blk = new Block(blk.fileName(), blk.number() - 1);
      pg.read(blk);
      currentrec = pg.getInt(LogMgr.LAST_POS);
   }
   
   
   //Task 2
   //Return current block and current record for FLogIterator
   public static Block getCurrentBlock() {
	   return blk;
   }
   
   public static Stack<Integer> getRecordOffset() {
	   return recordOffset;
   }
}
