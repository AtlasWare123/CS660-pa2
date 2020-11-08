package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    private PageId pageId;
    private int tuple_num;
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        this.pageId = pid;
        this.tuple_num = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        // some code goes here
        return this.tuple_num;
    }

    /**
     * @return the page id this RecordId references.
     *
     *
    */
    public PageId getPageId() {
        // some code goes here
        return this.pageId;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(!o.getClass().equals(o.getClass())){
            return false;
        }
        RecordId cmp = (RecordId) o;
        return  (this.tuple_num == ((RecordId) o).tuple_num
                && this.pageId.equals(cmp.pageId)
        )       ;

    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        return getHash(String.valueOf(pageId) + String.valueOf(tuple_num));
    }

    private int getHash(String str){
        int hash = 0;
        int base = 1000000;
        for(int i = 0; i < str.length(); i++){
            hash = (hash * 31 + str.charAt(i)) % base;
        }
        return hash;
    }
}
