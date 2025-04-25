package org.apache.kudu.replication.wrappedsource;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.connector.kudu.connector.KuduTableInfo;
import org.apache.flink.connector.kudu.connector.reader.KuduReaderConfig;
import org.apache.flink.connector.kudu.source.KuduSource;
import org.apache.flink.connector.kudu.source.enumerator.KuduSourceEnumeratorState;
import org.apache.flink.connector.kudu.source.split.KuduSourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.lang.reflect.Field;
import java.time.Duration;

public class MetricWrappedKuduSource<OUT> implements Source<OUT, KuduSourceSplit, KuduSourceEnumeratorState> {

    private final KuduSource<OUT> delegate;
    private final KuduReaderConfig readerConfig;
    private final KuduTableInfo tableInfo;
    private final Boundedness boundedness;
    private final Duration discoveryInterval;

    public MetricWrappedKuduSource(KuduSource<OUT> delegate) {
        this.delegate = delegate;
        this.boundedness = delegate.getBoundedness();

        try {
            this.readerConfig = getPrivateField(delegate, "readerConfig", KuduReaderConfig.class);
            this.tableInfo = getPrivateField(delegate, "tableInfo", KuduTableInfo.class);
            this.discoveryInterval = getPrivateField(delegate, "discoveryPeriod", Duration.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract fields from KuduSource via reflection", e);
        }
    }

    private <T> T getPrivateField(Object obj, String fieldName, Class<T> type) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, KuduSourceSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return delegate.createReader(readerContext);
    }

    @Override
    public SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> createEnumerator(SplitEnumeratorContext<KuduSourceSplit> context) {
        return new MetricWrappedKuduEnumerator(tableInfo, readerConfig, boundedness, discoveryInterval, context, null);
    }

    @Override
    public SplitEnumerator<KuduSourceSplit, KuduSourceEnumeratorState> restoreEnumerator(SplitEnumeratorContext<KuduSourceSplit> context,
                                                                                         KuduSourceEnumeratorState checkpoint) throws Exception {
        return new MetricWrappedKuduEnumerator(tableInfo, readerConfig, boundedness, discoveryInterval, context, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<KuduSourceSplit> getSplitSerializer() {
        return delegate.getSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<KuduSourceEnumeratorState> getEnumeratorCheckpointSerializer() {
        return delegate.getEnumeratorCheckpointSerializer();
    }
}
