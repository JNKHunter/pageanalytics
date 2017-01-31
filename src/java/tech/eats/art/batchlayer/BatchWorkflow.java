package tech.eats.art.batchlayer;

import backtype.cascading.tap.PailTap;
import backtype.hadoop.pail.Pail;
import backtype.hadoop.pail.PailSpec;
import backtype.hadoop.pail.PailStructure;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.BufferCall;
import cascading.operation.FunctionCall;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import cascalog.CascalogFunction;
import cascalog.ops.RandLong;
import jcascalog.Api;
import jcascalog.Fields;
import jcascalog.Option;
import jcascalog.Subquery;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import tech.eats.art.schema.*;
import tech.eats.art.tap.SplitDataPailStructure;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

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

    public static class EdgifyEquiv extends CascalogFunction {
        public void operate(FlowProcess process, FunctionCall call) {
            Data data = (Data) call.getArguments().getObject(0);
            EquivEdge equiv = data.get_dataunit().get_equiv();
            call.getOutputCollector().add(
                    new Tuple(equiv.get_id1(), equiv.get_id2()));
        }
    }

    public static void normalizeUserIds() throws IOException {
        Tap equivs = attributeTap("/tmp/swa/normalized_urls",
                DataUnit._Fields.EQUIV);
        Api.execute(Api.hfsSeqfile("/tmp/swa/equivs0"),
                new Subquery("?node1", "?node2")
                        .predicate(equivs, "_", "?data")
                        .predicate(new EdgifyEquiv(), "?data")
                        .out("?node1", "?node2"));
        int i = 1;
        while(true) {
            Tap progressEdgesSink = runUserIdNormalizationIteration(i);

            if(!new HadoopFlowProcess(new JobConf())
                    .openTapForRead(progressEdgesSink)
                    .hasNext()) {
                break;
            }
            i++;
        }

        Tap pageviews = attributeTap("/tmp/swa/normalized_urls",
                DataUnit._Fields.PAGE_VIEW);
        Object newIds = Api.hfsSeqfile("/tmp/swa/equivs" + i);
        Tap result = splitDataTap(
                "/tmp/swa/normalized_pageview_users");

        Api.execute(result,
                new Subquery("?normalized-pageview")
                        .predicate(newIds, "!!newId", "?person")
                        .predicate(pageviews, "_", "?data")
                        .predicate(new ExtractPageViewFields(), "?data")
                        .out("?userid", "?person", "?timestamp")
                        .predicate(new MakeNormalizedPageview(),
                                "!!newId", "?data").out("?normalized-pageview"));
    }

    public static Tap runUserIdNormalizationIteration(int i) {
        Object source = Api.hfsSeqfile(
                "/tmp/swa/equivs" + (i - 1));
        Object sink = Api.hfsSeqfile("/tmp/swa/equivs" + i);

        Object iteration = new Subquery(
                "?b1", "?node1", "?node2", "?is-new")
                .predicate(source, "?n1", "?n2")
                .predicate(new BidirectionalEdge(), "?n1", "?n2")
                .out("?b1", "?b2")
                .predicate(new IterateEdges(), "?b2")
                .out("?node1", "?node2", "?is-new");

        iteration = Api.selectFields(iteration,
                new Fields("?node1", "?node2", "?is-new"));

        Subquery newEdgeSet = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", "?is-new")
                .predicate(Option.DISTINCT, true);

        Tap progressEdgesSink = new Hfs(new SequenceFile(cascading.tuple.Fields.ALL),
                "/tmp/swa/equivs" + i + "-new");
        Subquery progressEdges = new Subquery("?node1", "?node2")
                .predicate(iteration, "?node1", "?node2", true);

        Api.execute(Arrays.asList((Object)sink, progressEdgesSink),
                Arrays.asList((Object)newEdgeSet, progressEdges));
        return progressEdgesSink;
    }

    public static class BidirectionalEdge extends CascalogFunction {
        public void operate(FlowProcess process, FunctionCall call) {
            Object node1 = call.getArguments().getObject(0);
            Object node2 = call.getArguments().getObject(1);
            if(!node1.equals(node2)) {
                call.getOutputCollector().add(
                        new Tuple(node1, node2));
                call.getOutputCollector().add(
                        new Tuple(node2, node1));
            }
        }
    }

    public static class IterateEdges extends CascalogBuffer {
        public void operate(FlowProcess process, BufferCall call) {
            PersonID grouped = (PersonID) call.getGroup()
                    .getObject(0);
            TreeSet<PersonID> allIds = new TreeSet<PersonID>();
            allIds.add(grouped);

            Iterator<TupleEntry> it = call.getArgumentsIterator();
            while(it.hasNext()) {
                allIds.add((PersonID) it.next().getObject(0));
            }

            Iterator<PersonID> allIdsIt = allIds.iterator();
            PersonID smallest = allIdsIt.next();
            boolean isProgress = allIds.size() > 2 &&
                    !grouped.equals(smallest);
            while(allIdsIt.hasNext()) {
                PersonID id = allIdsIt.next();
                call.getOutputCollector().add(
                        new Tuple(smallest, id, isProgress));
            }
        }
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
