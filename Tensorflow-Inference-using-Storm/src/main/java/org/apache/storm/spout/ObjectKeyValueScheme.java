package org.apache.storm.spout;

import com.google.common.collect.ImmutableMap;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.spout.RawScheme;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.util.List;

public class ObjectKeyValueScheme extends RawScheme implements KeyValueScheme {
    @Override
    public List<Object> deserializeKeyAndValue(ByteBuffer key, ByteBuffer value) {
        if (key == null) {
            return deserialize(value);
        }
        Object keyTuple = deserialize(key).get(0);
        Object valueTuple = deserialize(value).get(0);

        return new Values(ImmutableMap.of(keyTuple, valueTuple));
    }
}
