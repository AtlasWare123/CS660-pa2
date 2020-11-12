package simpledb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.pageNumber() < 0 || pid.pageNumber() >= this.numPages()) {
            throw new NoSuchElementException(String.format("Page number: %d doesn't exist", pid.pageNumber()));
        }
        RandomAccessFile fin = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            fin = new RandomAccessFile(this.file, "r");
            fin.skipBytes(pid.pageNumber() * BufferPool.getPageSize());
            fin.read(data, 0, data.length);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fin != null) {
                try {
                    fin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile fout = null;
        try {
            fout = new RandomAccessFile(this.file, "rw");
            fout.skipBytes(page.getId().pageNumber() * BufferPool.getPageSize());
            fout.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)((this.file.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0; i <= this.numPages(); i++) {
            HeapPageId hpId = new HeapPageId(getId(), i);  // HeapFile id is associated with table id
            HeapPage page = i < this.numPages() ?
                    (HeapPage) Database.getBufferPool().getPage(tid, hpId, Permissions.READ_WRITE) :
                    new HeapPage(new HeapPageId(this.getId(), i), HeapPage.createEmptyPageData());
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                // flush newly created page to disk
                if (i == this.numPages()) {
                    this.writePage(page);
                }
                return new ArrayList<>(Collections.singletonList(page));
            }
        }
        throw new DbException("Failed to insert this tuple");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != this.getId() || pid.pageNumber() >= this.numPages()) {
            throw new DbException("The tuple doesn't exist in this table");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singleton(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int curPage = -1;
            private Iterator<Tuple> iter = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.curPage = 0;
                while (this.curPage < numPages()) {
                    if (this.iter == null) {
                        this.iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), this.curPage), Permissions.READ_ONLY)).iterator();
                    }
                    if (!this.iter.hasNext()) {
                        this.curPage++;
                        this.iter = null;
                    } else {
                        return;
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.curPage < numPages() && this.iter != null && this.iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (this.curPage == -1) {
                    throw new NoSuchElementException("iterator has closed");
                }
                Tuple ret = null;
                if (this.iter.hasNext()) {
                    ret = this.iter.next();
                }
                if (!this.iter.hasNext()) {
                    this.curPage++;
                    while (this.curPage < numPages()) {
                        this.iter = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), this.curPage), Permissions.READ_ONLY)).iterator();
                        if (!this.iter.hasNext()) {
                            this.curPage++;
                        } else {
                            break;
                        }
                    }
                }
                if (ret == null) {
                    throw new NoSuchElementException("No element exist");
                }
                return ret;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                this.curPage = -1;
                this.iter = null;
            }
        };
    }

}


//package simpledb;
//
//import java.io.*;
//import java.util.*;
//
///**
// * HeapFile is an implementation of a DbFile that stores a collection of tuples
// * in no particular order. Tuples are stored on pages, each of which is a fixed
// * size, and the file is simply a collection of those pages. HeapFile works
// * closely with HeapPage. The format of HeapPages is described in the HeapPage
// * constructor.
// *
// * @see simpledb.HeapPage#HeapPage
// * @author Sam Madden
// */
//public class HeapFile implements DbFile {
//
//    private File file;
//    private TupleDesc tupleDesc;
//    private int page_num;
//    /**
//     * Constructs a heap file backed by the specified file.
//     *
//     * @param f
//     *            the file that stores the on-disk backing store for this heap
//     *            file.
//     */
//    public HeapFile(File f, TupleDesc td) {
//        this.file = f;
//        this.tupleDesc = td;
//        this.page_num = (int) file.length() / BufferPool.getPageSize();
//    }
//
//    /**
//     * Returns the File backing this HeapFile on disk.
//     *
//     * @return the File backing this HeapFile on disk.
//     */
//    public File getFile() {
//
//        return file;
//    }
//
//    /**
//     * Returns an ID uniquely identifying this HeapFile. Implementation note:
//     * you will need to generate this tableid somewhere ensure that each
//     * HeapFile has a "unique id," and that you always return the same value for
//     * a particular HeapFile. We suggest hashing the absolute file name of the
//     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
//     *
//     * @return an ID uniquely identifying this HeapFile.
//     */
//    public int getId() {
//        // some code goes here
//        return file.getAbsoluteFile().hashCode();
//    }
//
//    /**
//     * Returns the TupleDesc of the table stored in this DbFile.
//     *
//     * @return TupleDesc of this DbFile.
//     */
//    public TupleDesc getTupleDesc() {
//        // some code goes here
//        return this.tupleDesc;
//    }
//
//    // see DbFile.java for javadocs
//    // disk -> cache(buffer pool) -> other read request
//    public Page readPage(PageId pid) {
//        //find the offset of the page
//        Page result = null;
//        byte[] data = new byte[BufferPool.getPageSize()];
//        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
//            // page OFFSET HeapFile
//            int pos = pid.pageNumber() * BufferPool.getPageSize();
//            raf.seek(pos);
//            raf.read(data, 0, data.length);
//            result = new HeapPage((HeapPageId) pid, data);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return result;
//    }
//
//    // see DbFile.java for javadocs
//    public void writePage(Page page) throws IOException {
//        // some code goes here
//        // not necessary for lab1
//        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
//            raf.seek(page.getId().pageNumber() * BufferPool.getPageSize());
//            byte[] data = page.getPageData();
//            raf.write(data);
//        }
//    }
//
//    /**
//     * Returns the number of pages in this HeapFile.
//     */
//    public int numPages() {
//
//        return page_num;
//    }
//
//    // see DbFile.java for javadocs
//    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
//            throws DbException, IOException, TransactionAbortedException {
//        // some code goes here
//
//        // not necessary for lab1
//        ArrayList<Page> affectedPages = new ArrayList<>();
//        for(int i = 0; i < numPages(); i++){
//            // get pid
//            HeapPageId pid = new HeapPageId(getId(), i);
//            HeapPage page =  (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//            if(page.getNumEmptySlots() != 0){
//                page.insertTuple(t);
//                page.markDirty(true, tid);
//                affectedPages.add(page);
//                break;
//            }
//        }
//        // full page nothing added
//        if(affectedPages.size() == 0){
//            // create a new page and add it to disk
//            HeapPageId npid = new HeapPageId(getId(), numPages());
//            HeapPage emptyPage = new HeapPage(npid, HeapPage.createEmptyPageData());
//            page_num++;
//            writePage(emptyPage);
//            // added page has to be retrieved using buffer pool
//            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, npid, Permissions.READ_WRITE);
//            newPage.insertTuple(t);
//            newPage.markDirty(true, tid);
//            affectedPages.add(newPage);
//        }
//        return affectedPages;
//    }
//
//    // see DbFile.java for javadocs
//    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
//            TransactionAbortedException {
//        // some code goes here
//        PageId pid = t.getRecordId().getPageId();
//        if (pid.getTableId() != this.getId() || pid.pageNumber() >= this.numPages()) {
//            throw new DbException("The tuple doesn't exist in this table");
//        }
//        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
//        page.deleteTuple(t);
//        return new ArrayList<>(Collections.singleton(page));
//    }
//
//    // see DbFile.java for javadocs
//    public DbFileIterator iterator(TransactionId tid) {
//        DbFileIterator dbFileIterator = new HeapFileIterator(tid);
//        return dbFileIterator;
//    }
//
//    private class HeapFileIterator implements DbFileIterator{
//
//        private Iterator<Tuple> tupleIterator;
//        private TransactionId tid;
//        private int page_pos;
//
//        public HeapFileIterator(TransactionId tid){
//            this.tid = tid;
//        }
//
//        @Override
//        public void open() throws DbException, TransactionAbortedException {
//            page_pos = 0;
//            HeapPageId pid = new HeapPageId(getId(), page_pos);
//            //加载第一页的tuples
//            tupleIterator = getTuplesInPage(pid);
//        }
//
//        @Override
//        public boolean hasNext() throws DbException, TransactionAbortedException {
//            if (tupleIterator == null) {
//                //说明已经被关闭
//                return false;
//            }
//            //如果当前页还有tuple未遍历
//            if (tupleIterator.hasNext()) {
//                return true;
//            }
//            //如果遍历完当前页，测试是否还有页未遍历
//            //注意要减一，这里与for循环的一般判断逻辑（迭代变量<长度）不同，是因为我们要在接下来代码中将pagePos加1才使用
//            //如果不理解，可以自己举一个例子想象运行过程
//            if (page_pos < numPages() - 1) {
//                page_pos++;
//                HeapPageId pid = new HeapPageId(getId(), page_pos);
//                tupleIterator = getTuplesInPage(pid);
//                //这时不能直接return ture，有可能返回的新的迭代器是不含有tuple的
//                return tupleIterator.hasNext();
//            }
//            else {
//                return false;
//            }
//        }
//
//        @Override
//        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//            if (!hasNext()) {
//                throw new NoSuchElementException("not opened or no tuple remained");
//            }
//            return tupleIterator.next();
//        }
//
//        @Override
//        public void rewind() throws DbException, TransactionAbortedException {
//            open();
//        }
//
//        @Override
//        public void close() {
//            page_pos = 0;
//            tupleIterator = null;
//        }
//
//        public Iterator<Tuple> getTuplesInPage(HeapPageId pid) throws TransactionAbortedException, DbException {
//            // 不能直接使用HeapFile的readPage方法，而是通过BufferPool来获得page，理由见readPage()方法的Javadoc
//            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
//            return page.iterator();
//        }
//    }
//}
//
//
