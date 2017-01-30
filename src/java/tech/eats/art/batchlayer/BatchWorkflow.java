package tech.eats.art.batchlayer;

import jcascalog.Api;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jhunter on 1/29/17.
 */
public class BatchWorkflow {

    public static void setApplicationConf(){
        Map conf = new HashMap();
        String sers = "backtype.hadoop.ThriftSerialization," +
                "org.apache.hadoop.io.serializer.WritableSerialization";
        conf.put("io.serializations", sers);
        Api.setApplicationConf(conf);
    }
}
