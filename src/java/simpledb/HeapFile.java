package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;


    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }


    /**
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Page readPage(PageId pid) throws IllegalArgumentException {
        Page page = null;
        int offset = pid.pageNumber() * BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            raf.seek(offset);
            raf.read(data, 0, data.length);
            page = new HeapPage((HeapPageId) pid, data);
        } catch (IOException ignored) {
        }
        return page;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writePage(Page page) throws IOException {
        byte[] data = page.getPageData();
        RandomAccessFile onDiskFile = new RandomAccessFile(file, "rw");
        long pos = BufferPool.getPageSize() * page.getId().pageNumber();
        onDiskFile.seek(pos);
        onDiskFile.write(data);
        onDiskFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> dirtyPageTable = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pageId = new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
                page.insertTuple(t);
                dirtyPageTable.add(page);
                return dirtyPageTable;
            }
        }
        // didn't find a page with at least 1 empty slot to insert
        HeapPageId pageId = new HeapPageId(getId(), numPages());
        HeapPage page = new HeapPage(pageId, HeapPage.createEmptyPageData());
        writePage(page);
        page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        page.insertTuple(t);
        dirtyPageTable.add(page);
        return dirtyPageTable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        RecordId recordId = t.getRecordId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, recordId.getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> dirtyPageTable = new ArrayList<>();
        dirtyPageTable.add(page);
        return dirtyPageTable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            private Iterator<Tuple> pageIterator = null;
            private int pageNumber;

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (pageIterator == null) return null;
                // if as expected, should land at this line
                if (pageIterator.hasNext()) return pageIterator.next();
                if (pageNumber == numPages() - 1) return null;
                pageNumber++;
                pageIterator = getPageIterator();
                return readNext();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNumber = 0;
                pageIterator = getPageIterator();
            }

            @Override
            public void close() {
                super.close();
                pageIterator = null;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (pageIterator != null) open();
            }

            private Iterator<Tuple> getPageIterator() throws DbException, TransactionAbortedException {
                return ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pageNumber), Permissions.READ_ONLY))
                        .iterator();
            }
        };
    }
}

