package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private List<Field> fieldsValues;
    private RecordId recordId;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */


    public Tuple(TupleDesc td) {
        this.tupleDesc = td;
        fieldsValues = new ArrayList<>();
        int n = td.numFields();
        for(int i = 0; i < n; i++){
            fieldsValues.add(null);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void
    setRecordId(RecordId rid) {
        this.recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) throws NoSuchElementException {
        if(i < 0 || i >= fieldsValues.size()){
            throw new NoSuchElementException("invalid index");
        }
        this.fieldsValues.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) throws NoSuchElementException {
        // some code goes here
        if(i < 0 || i >= fieldsValues.size()){
            throw new NoSuchElementException("invalid index");
        }
        return fieldsValues.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for(Field field: fieldsValues)
            stringBuilder.append(field.toString()).append("\t");
        return stringBuilder.append("\n").toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return fieldsValues.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        return;
    }


    /**
     * Merge two Tuples into one
     *
     * @param tuple1 The first tuple
     * @param tuple2 The second tuple
     * @return the new Tuple
     */
    public static Tuple merge(Tuple tuple1, Tuple tuple2) {
        Tuple newTuple = new Tuple(TupleDesc.merge(tuple1.getTupleDesc(), tuple2.getTupleDesc()));
        for (int i=0; i<tuple1.getTupleDesc().numFields(); i++) {
            newTuple.setField(i, tuple1.getField(i));
        }
        int offset = tuple1.getTupleDesc().numFields();
        for (int i=0; i<tuple2.getTupleDesc().numFields(); i++) {
            newTuple.setField(offset + i, tuple2.getField(i));
        }
        return newTuple;
    }
}
