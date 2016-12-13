import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SQLContext;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import scala.Function0;
import scala.collection.mutable.Seq;


import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by jguenther on 09.12.2016.
 */
public class SparkProduction {

    public static void main(String[] args) throws IOException {
        Config con = ConfigFactory.load();

        SparkConf conf = new SparkConf().setAppName(con.getString("spark.app")).setMaster(con.getString("spark.master"));

        SparkSession sc = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext jc = JavaSparkContext.fromSparkContext(sc.sparkContext());

           Broadcast<List<FeatureWrapper>> plzLs = jc.broadcast(extractFeatures(con.getString("spark.plz.inputPLZ"), Filter.INCLUDE));

        //  SQLContext sqlContext = new SQLContext(sc);

       /* Dataset<Row> df = sqlContext.read()
                .format("com.databricks.spark.xml")
                .option("rowTag", "node").option("rootTag", "create")
                .load("346.xml");

        System.out.println(df.select(col("_id"), col("_lat"), col("tag._k")).where("tag._k contains 'amenity'").count());


        df.printSchema();
        System.out.println(plzLs.value().size());*/

        //Todo
        // String content = StringUtils.join(sc.read().textFile().collect());
        JavaPairRDD<String, String> re = jc.wholeTextFiles(con.getString(con.getString("spark.plz.inputDir")));


        //Todo: Check on Cluster
        Configuration hdfsConf = new Configuration();
        FileSystem fs = FileSystem.get(hdfsConf);
        Path recent = getMostRecentFile(con.getString("spark.plz.inputDir"), fs);

        while (recent != null) {
            FSDataInputStream in = null;

            in = fs.open(recent);
            OSMParser par = new OSMParser(in);

            List<MapNode> updatedNodes = par.getUpdateNodes();
            List<MapNode> deletedNodes = par.getDeleteNodes();
            List<MapNode> newNodes = par.getNewNodes();
            par.clean();
            in.close();

            List<FeatureWrapper> PLZ = extractFeatures(con.getString("spark.plz.inputPLZ"), Filter.INCLUDE);
            // possibly write only one file
            updatedNodes = matchPLZ(updatedNodes, PLZ);
            deletedNodes = matchPLZ(deletedNodes, PLZ);
            newNodes = matchPLZ(newNodes, PLZ);

            String pt = con.getString("outPath");
            LocalDateTime ts = LocalDateTime.now();

            sc.createDataFrame(updatedNodes, MapNode.class).write().parquet(pt + "\\update_" + ts);
            sc.createDataFrame(deletedNodes, MapNode.class).write().parquet(pt + "\\delete_" + ts);
            sc.createDataFrame(newNodes, MapNode.class).write().parquet(pt + "\\new_" + ts);
            sc.stop();
            fs.delete(recent, true);
        }
    }

    @SuppressWarnings("Duplicates")
     public static List<FeatureWrapper> extractFeatures(String path, Filter filter) throws IOException {
        List<FeatureWrapper> ls = new ArrayList<>();

        final Map<String, Object> map = new HashMap<>();
        //todo -> will break with HDFS
        map.put("url", (new File(path)).toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        final String typeName = dataStore.getTypeNames()[0];

        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            while (features.hasNext()) {
                SimpleFeature fs = features.next();
                ls.add(new FeatureWrapper(fs));
            }
            features.close();
        }
        dataStore.dispose();
        return ls;

    }

    private static Path getMostRecentFile(String path, FileSystem fs) throws IOException {

        MutablePair<Long, Path> curr = new MutablePair<>(0L, null);

        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path(path), false);
        while (files.hasNext()) {
            LocatedFileStatus st = files.next();
            if (st.getModificationTime() > curr.getKey()) {
                curr.setLeft(st.getModificationTime());
                curr.setRight(st.getPath());
            }
        }
        if (curr.getRight() == null) {
            throw new IOException("Path is empty!");
        }
        return curr.getRight();

    }

    private static List<MapNode> matchPLZ(List<MapNode> in, List<FeatureWrapper> PLZ) {
        Stream<MapNode> st = in.stream().map(el -> {
                    if (el.getPlz() == null) {
                        Integer plz = PLZ.stream().filter(pl -> pl.getBounds().contains(el.getBounds())).findFirst().get().getPlz();
                        el.setPlz(plz);
                    }
                    return el;
                }
        );
        return st.collect(Collectors.toList());
    }

}



