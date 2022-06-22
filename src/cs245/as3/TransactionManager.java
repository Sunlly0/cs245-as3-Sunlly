package cs245.as3;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

import java.nio.ByteBuffer;
import java.util.*;


public class TransactionManager {
	class WritesetEntry {
		public long key;
		public byte[] value;
		public WritesetEntry(long key, byte[] value) {
			this.key = key;
			this.value = value;
		}
	}
	/**
	  * Holds the latest value for each key.
	  */
	private HashMap<Long, TaggedValue> latestValues;
	/**
	  * Hold on to writesets until commit.
	  */
	private HashMap<Long, ArrayList<WritesetEntry>> writesets;
	// add struct by Sunlly
	private LogManager lm;
	private StorageManager sm;
	private HashMap<Long,Long> keyToTag;
//	private ArrayList<Long> Tags;
	//add method by Sunlly

	private HashMap<Long, ArrayList<LogRecord>> logRecordSets;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = new HashMap<>();

		//add by Sunlly
		logRecordSets= new HashMap<>();
		keyToTag=new HashMap<>();
//		Tags=new ArrayList<>();
	}

	/**
	 * Prepare the transaction manager to serve operations.
	 * At this time you should detect whether the StorageManager is inconsistent and recover it.
	 */
	public void initAndRecover(StorageManager sm, LogManager lm) {
		//初始化
		latestValues = sm.readStoredTable();
		this.lm=lm;
		this.sm=sm;

		ArrayList<LogRecord> records = new ArrayList<>();
		HashSet<Long> committedTxn = new HashSet<>();

		//从 logManager 中获取日志，读取日志记录
		int logEndOffset=lm.getLogEndOffset();

		for (int offset = lm.getLogTruncationOffset(); offset < logEndOffset; ) {
			byte[] sizeBytes = lm.readLogRecord(offset, 20);
			ByteBuffer wrap = ByteBuffer.wrap(sizeBytes);
			int type= wrap.getInt();
			int size = wrap.getInt();

			//解析日志，从 byte到结构体
			byte[] encodeRecord = lm.readLogRecord(offset, size);
			LogRecord logRecord = LogRecord.decode(encodeRecord);
			logRecord.setOffset(offset);

			//保存records
			records.add(logRecord);

			//保存已经提交的 txnId, 后续做 redo
			if (LogRecord.COMMIT == logRecord.getType()) {
				committedTxn.add(logRecord.getTxnId());
			}
			offset += logRecord.getSize();
		}
//		lm.setLogTruncationOffset(logEndOffset);

		// redo 已经提交的日志(恢复)
		for (LogRecord record : records) {
			long txnId = record.getTxnId();
			//遍历 records，如果在已经提交的事务列表中，并且是写操作，则做 redo
			if (committedTxn.contains(txnId) && record.getType() == LogRecord.WRITE ) {
				long tag = record.getOffset();
//				keyToTag.put(record.getKey(), tag);
//				Tags.add(tag);
				// 应用日志
				// debug by Sunlly
//				if(record.getKey()==2){
//					byte[] value=record.getValue();
//					System.out.println("redo: txnId:"+txnId+" key:"+record.getKey()+" tag:"+tag+" value:"+Arrays.toString(value));
//					if(txnId==3){
//						System.out.println("redo: txnId:"+txnId+" key:"+record.getKey()+" tag:"+tag+" value:"+Arrays.toString(value));
//					}
//				}
				//保存最新值
				latestValues.put(record.getKey(), new TaggedValue(tag, record.getValue()));
				//queueWrite: 修改了 lastVersion，但还没有真正持久化。（persisted_version）
				sm.queueWrite(record.getKey(), tag, record.getValue());
				keyToTag.put(record.getKey(), tag);
			}
		}
//		lm.setLogTruncationOffset(logEndOffset);
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
//		writesets.put(txID,new ArrayList<>());
//		logRecordSets.put(txID,new ArrayList<>());
	}

	/**
	 * Returns the latest committed value for a key by any transaction.
	 */
	public byte[] read(long txID, long key) {
		// 获取 key 的最新版本的值
		TaggedValue taggedValue = latestValues.get(key);
		return taggedValue == null ? null : taggedValue.value;
	}

	/**
	 * Indicates a write to the database. Note that such writes should not be visible to read() 
	 * calls until the transaction making the write commits. For simplicity, we will not make reads 
	 * to this same key from txID itself after we make a write to the key. 
	 */
	public void write(long txID, long key, byte[] value) {
		// by Sunlly
		//写 key-value，此时的写并没有被应用，而是保存到事务的写集中，等待commit时才真正被应用
		//同时通过日志将操作记录下来
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if(writeset==null){
			writeset=new ArrayList<>();
			writesets.put(txID,writeset);
		}
		writeset.add(new WritesetEntry(key, value));

		ArrayList<LogRecord> logRecordSet = logRecordSets.get(txID);
		if(logRecordSet==null){
			logRecordSet=new ArrayList<>();
			logRecordSets.put(txID,logRecordSet);
		}
		logRecordSet.add(new LogRecord(LogRecord.WRITE,24+ value.length,txID, key, value));

	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
  		// 增加 commit 的日志标识
		LogRecord commitRecord = new LogRecord(LogRecord.COMMIT,24, txID, -1, new byte[]{});
		ArrayList<LogRecord> logRecordSet = logRecordSets.get(txID);
		logRecordSet.add(commitRecord);

		HashMap<Long,Long> keyTags = new HashMap<>();

		//持久化日志
		for (LogRecord logRecord : logRecordSet) {

			byte[] encodeRecord = logRecord.encode(logRecord);
			long tag = lm.appendLogRecord(encodeRecord);
			// debug by Sunlly
//			if(logRecord.getKey()==2){
//				byte[] value=logRecord.getValue();
//				System.out.println("commit: txnId:"+txID+" key:"+logRecord.getKey()+" tag:"+tag+" value:"+Arrays.toString(value));
//				if(txID==3){
//					System.out.println(value);
//				}
//			}
//			keyTag.put(logRecord.getKey(), tag);
			keyTags.put(logRecord.getKey(),tag);
//			keyToTag.put(logRecord.getKey(),tag);
//			sm.queueWrite(logRecord.getKey(), tag, logRecord.getValue());
		}

		//持久化写操作
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
//				long tag=keyToTag.get(x.key);
				long tag=keyTags.get(x.key);
				latestValues.put(x.key, new TaggedValue(tag, x.value));
				sm.queueWrite(x.key, tag, x.value);
				keyToTag.put(x.key,tag);
			}
//				//tag is unused in this implementation:

//				Tags.add(tag);
//				// queueWrite 写入
//				sm.queueWrite(x.key, tag, x.value);
//				//使该修改对其他事务可见
//				latestValues.put(x.key, new TaggedValue(tag, x.value));
//				if (Tags.contains(tag)) {
//					Tags.remove(tag);
//				}
//			}
			writesets.remove(txID);
		}
	}
	/**
	 * Aborts a transaction.
	 */
	public void abort(long txID) {
		writesets.remove(txID);
		logRecordSets.remove(txID);
	}

	/**
	 * The storage manager will call back into this procedure every time a queued write becomes persistent.
	 * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
	 */

	// 找hashmap 的最小值 by Sunlly
	public static long getMinValue(HashMap<Long,Long> map) {
		Collection c = map.values();
		Object[] obj = c.toArray();
		Arrays.sort(obj);
		return Long.valueOf(String.valueOf(obj[0])).longValue();
	}

//	public static long getMinTag(ArrayList<Long> array) {
//		array.sort(Comparator.naturalOrder());
//		return array.get(0);
//	}

	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {

		// 如果 persisted_tag 是所有tag中的最小值，则可以截断
		//有些事务可能没有成功
		if (!keyToTag.isEmpty()) {
//		if (!Tags.isEmpty()) {
			long min_tag=this.getMinValue(keyToTag);
//			long min_tag=this.getMinTag(Tags);
			if (min_tag == persisted_tag && min_tag>=lm.getLogTruncationOffset()){
				lm.setLogTruncationOffset((int)persisted_tag);
//				lm.setLogTruncationOffset((int)min_tag);
//				keyToTag.remove(key,min_tag);
			}
		}
		keyToTag.remove(key,persisted_tag);
//		latestValues.put(key, new TaggedValue(persisted_tag, persisted_value));
//		Tags.remove(persisted_tag);

	}
}
