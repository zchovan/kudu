package org.apache.kudu.replication.wrappedsource;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumerator;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.metrics.Gauge;
import org.apache.kudu.util.HybridTimeUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.List;

public class MetricWrappedKuduEnumerator implements SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> {

    private final KuduSourceEnumerator delegate;
    private final SplitEnumeratorContext<KuduSourceSplit> context;

    private final Field lastEndTimestampField;
    private final Field pendingField;
    private final Field unassignedField;

    public MetricWrappedKuduEnumerator(
            KuduTableInfo tableInfo,
            KuduReaderConfig readerConfig,
            Boundedness boundedness,
            Duration discoveryInterval,
            SplitEnumeratorContext<KuduSourceSplit> context,
            @Nullable KuduSourceEnumeratorState restoredState
    ) {
        this.context = context;

        if (restoredState != null) {
            this.delegate = new KuduSourceEnumerator(tableInfo, readerConfig, boundedness, discoveryInterval, context, restoredState);
        } else {
            this.delegate = new KuduSourceEnumerator(tableInfo, readerConfig, boundedness, discoveryInterval, context);
        }

        try {
            this.lastEndTimestampField = delegate.getClass().getDeclaredField("lastEndTimestamp");
            this.lastEndTimestampField.setAccessible(true);

            this.pendingField = delegate.getClass().getDeclaredField("pending");
            this.pendingField.setAccessible(true);

            this.unassignedField = delegate.getClass().getDeclaredField("unassigned");
            this.unassignedField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Failed to access private fields in KuduSourceEnumerator", e);
        }

    }

    @Override
    public void start() {
        context.metricGroup().gauge("lastEndTimestamp", (Gauge<Long>) () -> getLastEndTimestamp());
        context.metricGroup().gauge("pendingSplits", (Gauge<Integer>) () -> getPendingSplits());
        context.metricGroup().gauge("unassignedSplits", (Gauge<Integer>) () -> getUnassignedSplits());
        delegate.start();
    }


    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        delegate.handleSplitRequest(subtaskId, requesterHostname);
    }

    @Override
    public void addSplitsBack(List<KuduSourceSplit> splits, int subtaskId) {
        delegate.addSplitsBack(splits, subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        delegate.addReader(subtaskId);
    }

    @Override
    public KuduSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        return delegate.snapshotState(checkpointId);
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            throw new RuntimeException("Error closing KuduSplitEnumerator", e);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        delegate.handleSourceEvent(subtaskId, sourceEvent);
    }

    private long getLastEndTimestamp() {
        try {
            Object valObj = lastEndTimestampField.get(delegate);
            if (valObj instanceof Long) {
                long hybridTime = (Long) valObj;
                long[] parsed = HybridTimeUtil.HTTimestampToPhysicalAndLogical(hybridTime);
                long epochMicros = parsed[0];
                return epochMicros / 1_000;
            }
            throw new IllegalStateException("Field 'lastEndTimestamp' is not a Long");

        } catch (Exception e) {
            throw new RuntimeException("Failed to access 'lastEndTimestamp' field reflectively", e);
        }
    }

    private int getPendingSplits() {
        try {
            Object valObj = pendingField.get(delegate);
            if (valObj instanceof List) {
                return ((List) valObj).size();
            }
            throw new IllegalStateException("Field 'pending' is not a List");
        } catch (Exception e) {
            throw new RuntimeException("Failed to access 'pending' field reflectively", e);
        }
    }

    private int getUnassignedSplits() {
        try {
            Object valObj = unassignedField.get(delegate);
            if (valObj instanceof List) {
                return ((List) valObj).size();
            }
            throw new IllegalStateException("Field 'unassigned' is not a List");
        } catch (Exception e) {
            throw new RuntimeException("Failed to access 'unassigned' field reflectively", e);
        }
    }
}
