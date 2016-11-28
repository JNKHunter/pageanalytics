package tech.eats.art.tap;

import backtype.hadoop.pail.PailStructure;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.List;

/**
 * Created by jhunter on 11/28/16.
 */
public abstract class ThriftPailStructure<T extends Comparable> implements PailStructure<T> {

    private transient TSerializer ser;
    private transient TDeserializer des;

    public TSerializer getSerializer() {
        if(ser == null) ser = new TSerializer();
        return ser;
    }

    public TDeserializer getDeserializer() {
        if(ser == null) des = new TDeserializer();
        return des;
    }

    public boolean isValidTarget(String... strings) {
        return false;
    }

    public T deserialize(byte[] record) {
        T ret = createThriftObject();
        try{
            getDeserializer().deserialize((TBase)ret, record);
        }catch(TException e){
            throw new RuntimeException(e);
        }
        return ret;
    }



    public byte[] serialize(T obj) {
        try{
            return getSerializer().serialize((TBase) obj);
        }catch (TException e){
            throw new RuntimeException(e);
        }
    }

    public List<String> getTarget(T t) {
        return null;
    }

    public Class getType() {
        return null;
    }

    protected abstract T createThriftObject();
}
