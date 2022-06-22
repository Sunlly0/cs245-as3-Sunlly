package cs245.as3;

import java.nio.ByteBuffer;

/**
 * You will implement this class.
 * <p>
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 * <p>
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * <p>
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class LogRecord {
    // 头部长度
//		public static final int headLen = 13;
//		// LogManager最大的byte数组长度
//		public static final int maxLen = 128;

    //		private int size;
    //type
    // write=0
    // commit=1
    // abort=2
    public static int WRITE = 0; //write
    public static int COMMIT = 1; // commit record
//    public static int ABORT = 2; // commit record
    private int type;
    private long txnId;
    private long key;
    private byte[] value;

    private int size;
    private int offset;

    public LogRecord(long txnId, long key, byte[] value) {
        this.txnId = txnId;
        this.key = key;
        this.value = value;
    }

    public LogRecord(int type, int size,long txnId, long key, byte[] value) {
        this.type = type;
        this.size= size;
        this.txnId = txnId;
        this.key = key;
        this.value = value;
    }

    public byte[] getValue() {
        return this.value;
    }
    public long getTxnId() {
        return this.txnId;
    }
    public long getKey() {
        return this.key;
    }
    public int getSize() {
        return this.size;
    }

    public int getType() {
        return this.type;
    }
    public int getOffset() {
        return this.offset;
    }

    public void setSize(int size) {
        this.size = size;
    }
    public void setOffset(int offset) {
        this.offset=offset;
    }

    public static byte[] encode(LogRecord l) {
        int size = 24 + l.value.length;
        l.setSize(size);
        ByteBuffer allocate = ByteBuffer.allocate(size);
        allocate.putInt(l.type)
                .putInt(l.size)
                .putLong(l.txnId)
                .putLong(l.key)
                .put(l.value);
        return allocate.array();
    }

    public static LogRecord decode(byte[] bytes) {
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        int type = wrap.getInt();
        int size = wrap.getInt();
        long tnxId = wrap.getLong();
        long key = wrap.getLong();
//		int state = wrap.getInt();
        byte[] values = new byte[size - wrap.position()];
//        byte[] values = new byte[size - wrap.position()];
        wrap.get(values);
        return new LogRecord(type, size, tnxId, key, values);
    }
}
