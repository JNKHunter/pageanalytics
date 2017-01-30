package tech.eats.art.batchlayer;

import backtype.cascading.tap.PailTap;
import backtype.hadoop.pail.Pail;
import cascading.tap.Tap;
import cascalog.ops.RandLong;
import jcascalog.Api;
import jcascalog.Subquery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tech.eats.art.schema.DataUnit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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


    public static PailTap attributeTap(String path, final DataUnit._Fields... fields){
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.attrs = new List[] {
                new ArrayList<String>(){{
                    for(DataUnit._Fields field: fields){
                        add("" + field.getThriftFieldId());
                    }
                }}
        };
        return new PailTap(path, opts);
    }
    
    public static Pail shred() throws IOException {
        PailTap source = new PailTap("/tmp/swa/snapshot");
        PailTap sink = splitDataTap("/tmp/swa/shredded");

        Subquery reduced = new Subquery("?rand", "?data")
                .predicate(source, "_", "?data-in")
                .predicate(new RandLong())
                .out("?rand");

        Api.execute(sink, new Subquery("?data")
            .predicate(reduced, "_", "?data"));

        Pail shreddedPail = new Pail("/tmp/swa/shredded");
        shreddedPail.consolidate();
        return shreddedPail;
    }

    private static PailTap splitDataTap(String s) {
    }

    private static void appendNewDataToMasterPail(Pail masterPail, Pail snapshotPail) {

    }

}
