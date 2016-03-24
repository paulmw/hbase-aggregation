package aggregation.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;


public class GroupComparatorUtils {

    public static int compareRow(byte [] left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    public static int compareRow(Cell left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getRowArray(), left.getRowOffset(), left.getRowLength(), right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }

    public static int compareFamily(byte [] left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }

    public static int compareFamily(Cell left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getFamilyArray(), left.getFamilyOffset(), left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(), right.getFamilyLength());
    }

    public static int compareQualifier(byte [] left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }

    public static int compareQualifier(Cell left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getQualifierArray(), left.getQualifierOffset(), left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(), right.getQualifierLength());
    }

    public static int compareTimestamp(Cell left, Cell right) {
        // The below older timestamps sorting ahead of newer timestamps looks
        // wrong but it is intentional. This is how HBase stores cells so that
        // the newest cells are found first.
        if (left.getTimestamp() < right.getTimestamp()) {
            return 1;
        } else if (left.getTimestamp() > right.getTimestamp()) {
            return -1;
        }
        return 0;
    }

    public static int compareTags(byte [] left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left, 0, left.length, right.getTagsArray(), right.getTagsOffset(), right.getTagsLength());
    }

    public static int compareTags(Cell left, Cell right) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(left.getTagsArray(), left.getTagsOffset(), left.getTagsLength(), right.getTagsArray(), right.getTagsOffset(), right.getTagsLength());
    }

    public static int compareTypeByte(Cell left, Cell right) {
        return (0xff & left.getTypeByte()) - (0xff & right.getTypeByte());
    }

    public static int compareTypeByte(byte left, Cell right) {
        return (0xff & left) - (0xff & right.getTypeByte());
    }
}
