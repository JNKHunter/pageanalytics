package tech.eats.art.tap;

import tech.eats.art.schema.Data;

import java.util.Collections;
import java.util.List;

/**
 * Created by jhunter on 11/28/16.
 */
public class DataPailStructure extends ThriftPailStructure<Data> {

    @Override
    public Class getType() {
        return Data.class;
    }

    @Override
    protected Data createThriftObject() {
        return new Data();
    }

    @Override
    public List<String> getTarget(Data data) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean isValidTarget(String... strings) {
        return true;
    }
}
