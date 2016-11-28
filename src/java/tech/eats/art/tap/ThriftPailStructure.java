package tech.eats.art.tap;

import backtype.hadoop.pail.PailStructure;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

import java.util.List;

/**
 * Created by jhunter on 11/28/16.
 */
public class ThriftPailStructure<T extends Comparable> implements PailStructure<T> {

    private transient TSerializer ser;
    private transient TDeserializer des;

    

    public boolean isValidTarget(String... strings) {
        return false;
    }

    public T deserialize(byte[] bytes) {
        return null;
    }

    public byte[] serialize(T t) {
        return new byte[0];
    }

    public List<String> getTarget(T t) {
        return null;
    }

    public Class getType() {
        return null;
    }
}
