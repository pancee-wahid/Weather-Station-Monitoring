package centralstation.bitcask;

public class RecentLocation {
    long fileID;
    short valueSize;
    long valueOffset;
    long timestamp;

    RecentLocation(long timestamp, long fileID, short valueSize, long valueOffset) {
        this.timestamp = timestamp;
        this.fileID = fileID;
        this.valueSize = valueSize;
        this.valueOffset = valueOffset;
    }
}