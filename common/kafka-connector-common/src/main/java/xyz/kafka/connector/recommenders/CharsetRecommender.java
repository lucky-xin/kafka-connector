package xyz.kafka.connector.recommenders;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

/**
 * CharsetRecommender
 *
 * @author luchaoxin
 * @version V 1.0
 * @since 2023-06-19
 */
public class CharsetRecommender implements ConfigDef.Recommender {
    private final List<Object> charsets;
    private final VisibleCallback visible;

    public CharsetRecommender(Iterable<String> charsets, VisibleCallback visible) {
        this.visible = visible;
        this.charsets = ImmutableList.copyOf(charsets);
    }

    @Override
    public List<Object> validValues(String s, Map<String, Object> map) {
        return this.charsets;
    }

    @Override
    public boolean visible(String s, Map<String, Object> map) {
        return this.visible.visible(s, map);
    }
}
