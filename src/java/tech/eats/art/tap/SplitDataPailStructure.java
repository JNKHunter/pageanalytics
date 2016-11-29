package tech.eats.art.tap;

import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import tech.eats.art.schema.Data;
import tech.eats.art.schema.DataUnit;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jhunter on 11/28/16.
 */
public class SplitDataPailStructure extends DataPailStructure {
    public static HashMap<Short, FieldStructure> validFieldMap = new HashMap<>();

    protected static interface FieldStructure{
        public boolean isValidTarget(String[] dirs);
        public void fillTarget(List<String> ret, Object val);
    }

    static {
        for(DataUnit._Fields k: DataUnit.metaDataMap.keySet()){
            FieldValueMetaData md = DataUnit.metaDataMap.get(k).valueMetaData;
            FieldStructure fieldStruct;
            if(md instanceof StructMetaData && ((StructMetaData) md).structClass.getName().endsWith("Property")){
                    fieldStruct = new PropertyStructure((StructMetaData) md).structClass);
            }else{
                fieldStruct = new EdgeStructure();
            }

            validFieldMap.put(k.getThriftFieldId(), fieldStruct);

        }
    }

    @Override
    public List<String> getTarget(Data object) {
        List<String> ret = new ArrayList<>();
        DataUnit du = object.get_dataunit();
        short id = du.getSetField().getThriftFieldId();
        ret.add("" + id);
        validFieldMap.get(id).fillTarget(ret, du.getFieldValue());
        return ret;
    }

    @Override
    public boolean isValidTarget(String[] dirs) {
        if(dirs.length == 0){
            return false;
        }

        try{
            short id = Short.parseShort(dirs[0]);
            FieldStructure s = validFieldMap.get(id);
            if(s == null)
                return false;
            else
                return s.isValidTarget(dirs);
        } catch(NumberFormatException e){
            return false;
        }
    }
}
