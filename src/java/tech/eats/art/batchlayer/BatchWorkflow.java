package tech.eats.art.batchlayer;

import backtype.cascading.tap.PailTap;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.PailSpec;
import backtype.hadoop.pail.PailStructure;
import cascading.flow.FlowProcess;
import cascading.operation.FunctionCall;
import cascading.tap.Tap;
import cascading.tuple.Tuple;
import cascalog.CascalogFunction;
import cascalog.ops.RandLong;
import jcascalog.Api;
import jcascalog.Subquery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tech.eats.art.schema.Data;
import tech.eats.art.schema.DataUnit;
import tech.eats.art.schema.PageID;
import tech.eats.art.tap.SplitDataPailStructure;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
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

    public static PailTap splitDataTap(String path) {
        PailTap.PailTapOptions opts = new PailTap.PailTapOptions();
        opts.spec = new PailSpec(
                (PailStructure) new SplitDataPailStructure());
        return new PailTap(path, opts);
    }

    public static void normalizeURLs(){
        Tap masterDataset = new PailTap("/data/master");
        Tap outTap = splitDataTap("/tmp/swa/normalized_urls");
    }

    private static void appendNewDataToMasterPail(Pail masterPail, Pail snapshotPail) throws IOException {
        Pail shreddedPail = shred();
        masterPail.absorb(shreddedPail);
    }

    public static class NormalizeURL extends CascalogFunction {
        public void operate(FlowProcess process, FunctionCall call) {
            Data data = ((Data) call.getArguments()
                    .getObject(0)).deepCopy();
            DataUnit du = data.get_dataunit();

            if(du.getSetField() == DataUnit._Fields.PAGE_VIEW) {
                normalize(du.get_page_view().get_page());
            } else if(du.getSetField() ==
                    DataUnit._Fields.PAGE_PROPERTY) {
                normalize(du.get_page_property().get_id());
            }
            call.getOutputCollector().add(new Tuple(data));
        }

        private void normalize(PageID page) {
            if(page.getSetField() == PageID._Fields.URL) {
                String urlStr = page.get_url();
                try {
                    URL url = new URL(urlStr);
                    page.set_url(url.getProtocol() + "://" +
                            url.getHost() + url.getPath());
                } catch(MalformedURLException e) {
                }
            }
        }

    }

}
