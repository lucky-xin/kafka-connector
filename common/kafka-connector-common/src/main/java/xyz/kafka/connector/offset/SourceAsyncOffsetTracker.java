package xyz.kafka.connector.offset;

import cn.hutool.core.map.MapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SourceAsyncOffsetTracker
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-03-08
 */
public class SourceAsyncOffsetTracker implements OffsetTracker<Map<String, ?>, Map<String, ?>, Map<String, ?>> {

    private static final Logger log = LoggerFactory.getLogger(SourceAsyncOffsetTracker.class);

    private final ConcurrentHashMap<Map<String, ?>, OffsetState> offsets = new ConcurrentHashMap<>(1024);
    private final String offsetField;
    private final AtomicLong numEntries = new AtomicLong();

    public SourceAsyncOffsetTracker(String offsetField) {
        this.offsetField = offsetField;
    }

    @Override
    public long numOffsetStateEntries() {
        return numEntries.get();
    }

    @Override
    public void updateOffsets() {
        log.trace("Updating offsets");
        offsets.values()
                .stream()
                .parallel()
                .filter(OffsetState::isProcessed)
                .forEach(c -> numEntries.decrementAndGet());
        log.trace("Updated offsets, num entries: {}", numEntries);
    }

    @Override
    public OffsetState pendingRecord(Map<String, ?> sourceOffset) {
        log.trace("Adding pending record");
        OffsetState offsetStateOld = offsets.get(sourceOffset);
        Long offset = MapUtil.get(sourceOffset, this.offsetField, Long.class, -1L);
        if (offsetStateOld != null && (offsetStateOld.offset() >= offset)) {
            return offsetStateOld;
        }
        AsyncOffsetState offsetState = new AsyncOffsetState(offset);
        offsets.replace(sourceOffset, offsetState);
        numEntries.incrementAndGet();
        return offsetState;
    }

    @Override
    public Map<Map<String, ?>, Map<String, ?>> offsets(Map<Map<String, ?>, Map<String, ?>> partitionAndOffset) {
        Map<Map<String, ?>, Map<String, ?>> offset = new HashMap<>(offsets.size());
        Iterator<Map.Entry<Map<String, ?>, OffsetState>> itr = offsets.entrySet().iterator();
        while (itr.hasNext()) {
            Map.Entry<Map<String, ?>, OffsetState> entry = itr.next();
            OffsetState offsetState = entry.getValue();
            Map<String, ?> sourceOffset = entry.getKey();
            if (offsetState.isProcessed()) {
                itr.remove();
                offset.put(sourceOffset, Map.of(offsetField, offsetState.offset() + 1));
            }
        }
        return offset;
    }

    @Override
    public Optional<Map<String, ?>> lowestWatermarkOffset() {
        return offsets.entrySet()
                .stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey);
    }
}
