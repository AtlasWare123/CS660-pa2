package simpledb;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private final Predicate p;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        return p;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TupleDesc getTupleDesc() {
        return child.getTupleDesc();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void open() throws DbException, NoSuchElementException, TransactionAbortedException {
        super.open();
        child.open();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        super.close();
        child.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    @Override
    protected Tuple fetchNext() throws NoSuchElementException, TransactionAbortedException, DbException {
        while (child.hasNext()) {
            Tuple t = child.next();
            if (p.filter(t)) return t;
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{child};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setChildren(DbIterator[] children) {
        child = children[0];
    }
}
