package simpledb.buffer;

import simpledb.file.*;
import simpledb.server.SimpleDB;

import java.util.*;

/**
 * Manages the pinning and unpinning of buffers to blocks.
 * @author Edward Sciore
 *
 */
class BasicBufferMgr {
   private Buffer[] bufferpool;
   private int numAvailable;
   //Task 1
   //buffer pool data structure: HashMap bufferPoolMap
   private Map<String, Buffer> bufferPoolMap;
   //replacement strategy data structure: LinkedList bufferQueue
   private List<Block> bufferQueue;
   
   /**
    * Creates a buffer manager having the specified number 
    * of buffer slots.
    * This constructor depends on both the {@link FileMgr} and
    * {@link simpledb.log.LogMgr LogMgr} objects 
    * that it gets from the class
    * {@link simpledb.server.SimpleDB}.
    * Those objects are created during system initialization.
    * Thus this constructor cannot be called until 
    * {@link simpledb.server.SimpleDB#initFileAndLogMgr(String)} or
    * is called first.
    * @param numbuffs the number of buffer slots to allocate
    */
   BasicBufferMgr(int numbuffs) {
      bufferpool = new Buffer[numbuffs];
      numAvailable = numbuffs;
      for (int i=0; i<numbuffs; i++) {
         bufferpool[i] = new Buffer();
      }
      bufferPoolMap = new HashMap<String, Buffer>();
      bufferQueue = new LinkedList<Block>();
   }
   
   /**
    * Flushes the dirty buffers modified by the specified transaction.
    * @param txnum the transaction's id number
    */
   synchronized void flushAll(int txnum) {
      for (Buffer buff : bufferpool)
         if (buff.isModifiedBy(txnum))
         buff.flush();
   }
   
   /**
    * Pins a buffer to the specified block. 
    * If there is already a buffer assigned to that block
    * then that buffer is used;  
    * otherwise, an unpinned buffer from the pool is chosen.
    * Returns a null value if there are no available buffers.
    * @param blk a reference to a disk block
    * @return the pinned buffer
    */
//   synchronized Buffer pin(Block blk) {
//      Buffer buff = findExistingBuffer(blk);
//      if (buff == null) {
//         buff = chooseUnpinnedBuffer();
//         if (buff == null) 
//            return null;
//         buff.assignToBlock(blk);
//         
//      }
//      if (!buff.isPinned())
//         numAvailable--;
//      buff.pin();
//      return buff;
//   }
   
   synchronized Buffer pin(Block blk) {
      Buffer buff = findExistingBuffer(blk);
      if (buff == null) {
    	 //The blk is not in the buffer pool, write it into the buffer pool
         buff = chooseUnpinnedBuffer();
         if (buff == null) {
        	//currently no buffer available
    		 return null;
         }
         //buff can be an empty buffer, or a buffer unpinned
         buff.assignToBlock(blk);
         saveMapping(blk, buff);
         bufferQueue.add(blk);
      }
      if (!buff.isPinned()) {
         numAvailable--;
      }
      buff.pin();
      
      return buff;
   }
   
   /**
    * Allocates a new block in the specified file, and
    * pins a buffer to it. 
    * Returns null (without allocating the block) if 
    * there are no available buffers.
    * @param filename the name of the file
    * @param fmtr a pageformatter object, used to format the new block
    * @return the pinned buffer
    */
//   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
//      Buffer buff = chooseUnpinnedBuffer();
//      if (buff == null)
//         return null;
//      buff.assignToNew(filename, fmtr);
//      numAvailable--;
//      buff.pin();
//      return buff;
//   }
   
   synchronized Buffer pinNew(String filename, PageFormatter fmtr) {
      Buffer buff = chooseUnpinnedBuffer();
      if (buff == null)
         return null;
      buff.assignToNew(filename, fmtr);
      numAvailable--;
      buff.pin();
      
      FileMgr filemgr = SimpleDB.fileMgr();
      int blknum = filemgr.size(filename);;
      Block blk = new Block(filename, blknum);
      saveMapping(blk, buff);
      bufferQueue.add(blk);
      
      return buff;
   }
   
   /**
    * Unpins the specified buffer.
    * @param buff the buffer to be unpinned
    */
   synchronized void unpin(Buffer buff) {
      buff.unpin();
      if (!buff.isPinned())
         numAvailable++;
   }
   
   /**
    * Returns the number of available (i.e. unpinned) buffers.
    * @return the number of available buffers
    */
   int available() {
      return numAvailable;
   }
   
//   private Buffer findExistingBuffer(Block blk) {
//      for (Buffer buff : bufferpool) {
//         Block b = buff.block();
//         if (b != null && b.equals(blk))
//            return buff;
//      }
//      return null;
//   }
//   
//   private Buffer chooseUnpinnedBuffer() {
//      for (Buffer buff : bufferpool)
//         if (!buff.isPinned())
//         return buff;
//      return null;
//   }
   
   private Buffer findExistingBuffer(Block blk) {
	   return getMapping(blk);
   }
   
   private Buffer chooseUnpinnedBuffer() {
	   //buffer pool is not full, write block to the next empty buffer
	   if(bufferPoolMap.size() < bufferpool.length) {
		   return bufferpool[bufferPoolMap.size()];
	   }
	   //no available buffer in the buffer pool
	   if(numAvailable == 0) {
		   return null;
	   }
	   //find the first buffer not pinned in the queue
	   for(int index = 0; index < bufferQueue.size(); index++) {
		   Buffer buffer = getMapping(bufferQueue.get(index));
		   if(!buffer.isPinned()) {
			   //replace this buffer
			   Block oldBlk = bufferQueue.get(index);
			   bufferQueue.remove(index);
			   bufferPoolMap.remove(getBlockKey(oldBlk));
			   return buffer;
		   }
	   }
	   return null;
   }
   
   /*********************bufferPoolMap Methods**************************/
   /**
    * Determines whether the map has a mapping from
    * the block to some buffer.
    * @param blk the block to use as a key
    * @return true if there is a mapping; false otherwise
    */
   boolean containsMapping(Block blk) {
	   String key = getBlockKey(blk);
	   return bufferPoolMap.containsKey(key);
   }
   /**
    * Returns the buffer that the map maps the specified block to.
    * @param blk the block to use as a key
    * @return the buffer mapped to if there is a mapping; null otherwise 
    */
   Buffer getMapping(Block blk) {
	   if(!containsMapping(blk)) {
		   return null;
	   }
	   String key = getBlockKey(blk);
	   return bufferPoolMap.get(key);
   }
   
   /**
    * Return the key for this block
    * @param blk
    * @return the key for blk
    */
   String getBlockKey(Block blk) {
	   return blk.fileName() + blk.number();
   }
   
   /**
    * Save the mapping in the bufferPoolMap
    * @param blk key
    * @param buf value
    */
   void saveMapping(Block blk, Buffer buf) {
	   String key = getBlockKey(blk);
	   bufferPoolMap.put(key, buf);
   }
   
   /*****************Test******************/
   synchronized Buffer[] pool() {
	   return bufferpool;
   }
   
   void clearBufferPool() {
	   for(int i = 0; i < bufferpool.length; i++) {
		   bufferpool[i] = new Buffer();
	   }
	   numAvailable = bufferpool.length;
	   bufferPoolMap.clear();
	   bufferQueue.clear();
   }
   
   void printQueue() {
	   for(Block blk : bufferQueue) {
		   System.out.println(blk.number());
	   }
   }
   
   void printBufferPool() {
	   for(Buffer buf : bufferpool) {
		   System.out.println(buf.block().number());
	   }
   }
   
}
