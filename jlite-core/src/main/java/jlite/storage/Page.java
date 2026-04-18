package jlite.storage;
/**
 * TODO: slotted page layout — header, slot array, tuple data.
 * TODO: insert/delete/compact tuples.
 */
public class Page {
    public static final int PAGE_SIZE = 4096;
    private final int pageId;
    private final byte[] data;
    public Page(int pageId) { this.pageId = pageId; this.data = new byte[PAGE_SIZE]; }
    public int getPageId() { return pageId; }
    public byte[] getData() { return data; }
}
