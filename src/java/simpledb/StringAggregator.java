package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUPING_FIELD = new IntField(0);

    private final int gbField;
    private final int aField;
    private final Type gbFieldType;
    private final Op what;
    private final Map<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbField     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aField      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        if (what != Op.COUNT) throw new IllegalArgumentException("StringAggregator only support COUNT");
        this.gbField = gbField;
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByKey = gbField != NO_GROUPING ? tup.getField(gbField) : NO_GROUPING_FIELD;
        groupMap.put(groupByKey, 1 + groupMap.getOrDefault(groupByKey, 0));
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    @Override
    public DbIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td = getTupleDesc();
        if (gbField == NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(groupMap.get(NO_GROUPING_FIELD)));
            tuples.add(tuple);
        } else {
            for (Field field : groupMap.keySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, field);
                tuple.setField(1, new IntField(groupMap.get(field)));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    private TupleDesc getTupleDesc() {
        return gbField != NO_GROUPING ? new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE}) : new TupleDesc(new Type[]{Type.INT_TYPE});
    }

}
