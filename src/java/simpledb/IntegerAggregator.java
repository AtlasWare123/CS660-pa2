package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUPING_FIELD = new IntField(0);

    private final int gbField;
    private final int aField;
    private final Type gbFieldType;
    private final Op what;
    private final Map<Field, Group> groupMap;

    /**
     * Aggregate constructor
     *
     * @param gbField     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param aField      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    public IntegerAggregator(int gbField, Type gbFieldType, int aField, Op what) {
        this.gbField = gbField;
        this.gbFieldType = gbFieldType;
        this.aField = aField;
        this.what = what;
        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    @Override
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField field = (IntField) tup.getField(aField);
        int value = field.getValue();
        Field groupByKey = gbField != NO_GROUPING ? tup.getField(gbField) : NO_GROUPING_FIELD;
        Group group = groupMap.get(groupByKey);
        if (group != null) {
            // map contains the group
            switch (what) {
                case MIN:
                    group.value = Math.min(group.value, value);
                    break;
                case MAX:
                    group.value = Math.max(group.value, value);
                    break;
                case SUM:
                case AVG:
                case COUNT:
                    group.value += value;
                    group.count++;
                    break;
                default:
                    throw new UnsupportedOperationException("IntegerAggregator only supports MIN, MAX, SUM, AVG, COUNT");
            }
        } else {
            // a brand new group
            switch (what) {
                case MIN:
                case MAX:
                case SUM:
                case AVG:
                case COUNT:
                    group = new Group(value, 1);
                    break;
                default:
                    throw new UnsupportedOperationException("IntegerAggregator only supports MIN, MAX, SUM, AVG, COUNT");
            }
            groupMap.put(groupByKey, group);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    @Override
    public DbIterator iterator() {
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td = getTupleDesc();
        if (gbField == NO_GROUPING) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(getAggregateResult(groupMap.get(NO_GROUPING_FIELD))));
            tuples.add(tuple);
        } else {
            for (Field field : groupMap.keySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, field);
                tuple.setField(1, new IntField(getAggregateResult(groupMap.get(field))));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    /**
     * @return the aggregate result
     */
    private int getAggregateResult(Group group) {
        switch (what) {
            case MIN:
            case MAX:
            case SUM:
                return group.value;
            case AVG:
                return group.value / group.count;
            case COUNT:
                return group.count;
            default:
                throw new UnsupportedOperationException("IntegerAggregator only supports MIN, MAX, SUM, AVG, COUNT");
        }
    }

    private TupleDesc getTupleDesc() {
        return new TupleDesc(gbField != NO_GROUPING ? new Type[]{gbFieldType, Type.INT_TYPE} : new Type[]{Type.INT_TYPE});
    }


    private static class Group {
        int value;
        int count;

        public Group(int value, int count) {
            this.value = value;
            this.count = count;
        }
    }

}
