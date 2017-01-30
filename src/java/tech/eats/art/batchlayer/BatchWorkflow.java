package tech.eats.art.batchlayer;

import backtype.hadoop.pail.Pail;
import jcascalog.Api;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jhunter on 1/29/17.
 */
public class BatchWorkflow {

    private final static String NEW_DATA_SNAPSHOT = "/tmp/swa/newDataSnapshot";

    public static void setApplicationConf(){
        Map conf = new HashMap();
        String sers = "backtype.hadoop.ThriftSerialization," +
                "org.apache.hadoop.io.serializer.WritableSerialization";
        conf.put("io.serializations", sers);
        Api.setApplicationConf(conf);
    }

    public static void ingest(Pail masterPail, Pail newDataPail) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path("/tmp/swa"), true);
        fs.mkdirs(new Path("tmp/swa"));

        Pail snapshotPail = newDataPail.snapshot(NEW_DATA_SNAPSHOT);
        appendNewDataToMasterPail(masterPail, snapshotPail);
    }

    private static void appendNewDataToMasterPail(Pail masterPail, Pail snapshotPail) {

    }
}
