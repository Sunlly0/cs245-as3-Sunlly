package cs245.as3;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;


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
	private PriorityQueue<Long> offsetQueue;

	//add method by Sunlly

	private HashMap<Long, ArrayList<LogRecord>> logRecordSets;

	public TransactionManager() {
		writesets = new HashMap<>();
		//see initAndRecover
		latestValues = null;

		//add by Sunlly
		logRecordSets= new HashMap<>();

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
		for (int offset = lm.getLogTruncationOffset(); offset < lm.getLogEndOffset(); ) {
			byte[] sizeBytes = lm.readLogRecord(offset, 4);
			int size = ByteBuffer.wrap(sizeBytes).getInt();

			//解析日志，从 byte到结构体
			byte[] decodeRecode = lm.readLogRecord(offset, size);
			LogRecord logRecord = LogRecord.decode(decodeRecode);
			logRecord.setOffset(offset);

			//保存records
			records.add(logRecord);

			//保存已经提交的 txnId, 后续做 redo
			if (LogRecord.COMMIT == logRecord.getType()) {
				committedTxn.add(logRecord.getTxnId());
			}
			offset += logRecord.getSize();
		}

		// redo 已经提交的日志(恢复)
		for (LogRecord record : records) {
			long txnId = record.getTxnId();
			//遍历 records，如果在已经提交的事务列表中，并且是写操作，则做 redo
			if (committedTxn.contains(txnId) && record.getType() == LogRecord.WRITE ) {
				int tag = record.getOffset();
//				offsetQueue.offer(tag);
				// 应用日志
				sm.queueWrite(record.getKey(), tag, record.getValue());
				//保存最新值
				latestValues.put(record.getKey(), new TaggedValue(tag, record.getValue()));
			}
		}
	}

	/**
	 * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
	 */
	public void start(long txID) {
		// TODO: Not implemented for non-durable transactions, you should implement this
		writesets.put(txID,new ArrayList<>());
		logRecordSets.put(txID,new ArrayList<>());
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
		ArrayList<LogRecord> logRecordSet = logRecordSets.get(txID);
		logRecordSet.add(new LogRecord(txID, key, value));
		logRecordSets.put(txID, logRecordSet);

		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		writeset.add(new WritesetEntry(key, value));
		writesets.put(txID, writeset);
	}
	/**
	 * Commits a transaction, and makes its writes visible to subsequent read operations.\
	 */
	public void commit(long txID) {
		//持久化日志
		LogRecord commitRecord = new LogRecord(LogRecord.COMMIT,txID, -1, new byte[]{});
		ArrayList<LogRecord> logRecordSet = logRecordSets.get(txID);
		logRecordSet.add(commitRecord);

		for (LogRecord logRecord : logRecordSet) {
			byte[] encodeRecord = logRecord.encode(logRecord);
			lm.appendLogRecord(encodeRecord);
//			keyToOffset.put(logRecord.getKey(), offset);
		}

		logRecordSets.put(txID, logRecordSet);

		//持久化
		ArrayList<WritesetEntry> writeset = writesets.get(txID);
		if (writeset != null) {
			for(WritesetEntry x : writeset) {
				//tag is unused in this implementation:
				long tag = 0;
				// queueWrite 写入
				sm.queueWrite(x.key, tag, x.value);
				latestValues.put(x.key, new TaggedValue(tag, x.value));
			}
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
	public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
		if (persisted_tag == offsetQueue.peek()) {
			lm.setLogTruncationOffset((int)persisted_tag);
		}
		offsetQueue.remove(persisted_tag);
	}
}
