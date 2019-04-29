package fr.client.utils;

/**
 * Pairs for offset and length
 *
 */
public class OffLenPair {
    public long offset;
    public long length;

    public OffLenPair(long offset, long length) {
        this.offset = offset;
        this.length = length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OffLenPair that = (OffLenPair) o;
        return offset == that.offset &&
                length == that.length;
    }

}
