package simpledb;

import java.util.NoSuchElementException;

import static simpledb.Aggregator.NO_GROUPING;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final int aField;
    private final int gField;
    private final Aggregator.Op aop;

    private DbIterator currIter;
    private DbIterator child;
    private TupleDesc td;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of aField, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The DbIterator that is feeding us tuples.
     * @param aField The column over which we are computing an aggregate.
     * @param gField The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(DbIterator child, int aField, int gField, Aggregator.Op aop) {
        this.child = child;
        this.aField = aField;
        this.gField = gField;
        this.aop = aop;
        this.td = updateTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples If not, return null;
     */
    public String groupFieldName() {
        return gField == NO_GROUPING ? null : child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        super.open();

        // generate current iterator
        Type gFieldType = gField != NO_GROUPING ? child.getTupleDesc().getFieldType(gField) : null;
        Aggregator aggregator;
        switch (child.getTupleDesc().getFieldType(aField)) {
            case INT_TYPE:
                aggregator = new IntegerAggregator(gField, gFieldType, aField, aop);
                break;
            case STRING_TYPE:
                aggregator = new StringAggregator(gField, gFieldType, aField, aop);
                break;
            default:
                throw new UnsupportedOperationException("only support integer and string");
        }
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        currIter = aggregator.iterator();

        currIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        return currIter.hasNext() ? currIter.next() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        super.close();
        super.open();
        currIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        return td;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        super.close();
        currIter.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
        td = updateTupleDesc();
    }

    private TupleDesc updateTupleDesc() {
        TupleDesc ctd = child.getTupleDesc();
        return gField != NO_GROUPING ?
                new TupleDesc(new Type[]{ctd.getFieldType(gField), ctd.getFieldType(aField)})
                : new TupleDesc(new Type[]{ctd.getFieldType(aField)});
    }
}
