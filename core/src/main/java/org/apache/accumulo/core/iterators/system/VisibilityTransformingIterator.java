package org.apache.accumulo.core.iterators.system;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public abstract class VisibilityTransformingIterator implements SortedKeyValueIterator<Key,Value> {

    private SortedKeyValueIterator<Key, Value> source;

    private SortedMap<Key,Value> vtiKvPairs = new TreeMap<>();

    private final Text rowHolder = new Text();
    private final Text cfHolder = new Text();
    private final Text cqHolder = new Text();

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        this.source = source;
    }

    @Override
    public boolean hasTop() {
        return !vtiKvPairs.isEmpty() || source.hasTop();
    }

    @Override
    public void next() throws IOException {
        vtiKvPairs = vtiKvPairs.tailMap(vtiKvPairs.firstKey());
        if (vtiKvPairs.isEmpty()) {
            source.next();
            transformSource();
        }
    }

    private void transformSource() {
        Key topKey = source.getTopKey();
        Value topVal = source.getTopValue();
        vtiKvPairs.put(topKey, topVal);
        for (Map.Entry<ColumnVisibility, Value> transformed: transformVisibility(topKey, topVal)) {
            vtiKvPairs.put(replaceVisibility(topKey, transformed.getKey()), transformed.getValue());
        }
    }

    private Key replaceVisibility(Key key, ColumnVisibility newVis) {
        return new Key(key.getRow(rowHolder), key.getColumnFamily(cfHolder), key.getColumnQualifier(cqHolder), newVis, key.getTimestamp());
    }

    public static ColumnVisibility replaceTerm(ColumnVisibility vis, String oldTerm, String newTerm) {
        newTerm = ColumnVisibility.quote(newTerm);
        ByteSequence oldTermBs = new ArrayByteSequence(oldTerm.getBytes());
        ColumnVisibility.Node root = vis.getParseTree();
        byte[] expression = vis.getExpression();
        StringBuilder out = new StringBuilder();
        stringify(newTerm, oldTermBs, root, expression, out);
        return new ColumnVisibility(out.toString());
    }

    private static void stringify(String newTerm, ByteSequence oldTermBs, ColumnVisibility.Node root, byte[] expression, StringBuilder out) {
        if (root.getType() == ColumnVisibility.NodeType.TERM) {
            ByteSequence termBs = root.getTerm(expression);
            if (termBs.compareTo(oldTermBs) == 0) {
                out.append(newTerm);
            } else {
                out.append(termBs);
            }
        } else {
            String sep = "";
            for (ColumnVisibility.Node c : root.getChildren()) {
                out.append(sep);
                boolean parens = (c.getType() != ColumnVisibility.NodeType.TERM && root.getType() != c.getType());
                if (parens)
                    out.append("(");
                stringify(newTerm, oldTermBs, c, expression, out);
                if (parens)
                    out.append(")");
                sep = root.getType() == ColumnVisibility.NodeType.AND ? "&" : "|";
            }
        }
    }

    protected abstract Iterable<Map.Entry<ColumnVisibility, Value>> transformVisibility(Key key, Value value);

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        Range adjusted = range;
        if (!range.isInfiniteStartKey()) {
            // The start key of the range might be a transformed key, that is, it might not be present in the source
            // iterator. Seek to the specified (row, cf, cq) and start transforming from there, throwing out anything
            // prior to the specified start key. Complicated a little bit by the fact that visibility sorts before
            // timestamp -- we might have to throw out a bunch of different versions of the key before we start
            // producing useful data.
            Key startKey = range.getStartKey();
            startKey = new Key(startKey.getRow(rowHolder), startKey.getColumnFamily(cfHolder),
                    startKey.getColumnQualifier(cqHolder));
            adjusted = new Range(startKey, range.getEndKey());
        }
        source.seek(adjusted, columnFamilies, inclusive);
        transformSource();
        if (!adjusted.equals(range)) {
            while (getTopKey().compareTo(range.getStartKey()) < 0) {
                next();
            }
        }
    }

    @Override
    public Key getTopKey() {
        return vtiKvPairs.firstKey();
    }

    @Override
    public Value getTopValue() {
        return vtiKvPairs.get(vtiKvPairs.firstKey());
    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        VisibilityTransformingIterator vtiIter;
        try {
            vtiIter = getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Subclasses of VisibilityTransformingIterator must define a no-arg constructor", e);
        }
        vtiIter.source = source.deepCopy(env);
        return vtiIter;
    }
}
