package jlite.wal;
/** TODO: LSN, serialise/deserialise to byte buffer. */
public record WalRecord(long lsn, WalRecordType type, long transactionId, byte[] payload) {}
