import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by jguenther on 13.12.2016.
 */
public class MRStyle {

    public static void main(String[] args) throws IOException {

        Config con = ConfigFactory.load();

        SparkConf conf = new SparkConf().setAppName(con.getString("spark.app")).setMaster(con.getString("spark.master"));

        SparkSession sc = SparkSession.builder().config(conf).getOrCreate();
        JavaSparkContext jc = JavaSparkContext.fromSparkContext(sc.sparkContext());
        // support wildcards
        JavaPairRDD<String, MapNode> ze = jc.wholeTextFiles(con.getString("spark.plz.inputDir")).flatMapToPair((PairFlatMapFunction<Tuple2<String, String>, String, MapNode>) stringStringTuple2 -> {
            OSMParser os = new OSMParser(stringStringTuple2._2());
            os.start();
            List<Tuple2<String, MapNode>> results = new ArrayList<>();
            os.getUpdateNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getType(), e)));
            os.getNewNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getType(), e)));
            os.getDeleteNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getType(), e)));
            return results.iterator();
        });

        Broadcast<List<FeatureWrapper>> plzLs = jc.broadcast(extractFeatures(con.getString("spark.plz.inputPLZ"), Filter.INCLUDE));


        JavaRDD<Tuple2<String, MapNode>> mapped = ze.map((Function<Tuple2<String, MapNode>, Tuple2<String, MapNode>>) t -> {
            FeatureWrapper match = plzLs.value().stream().filter(e -> e.getBounds().contains(t._2().getBounds())).findFirst().get();
            MapNode tmp = t._2();
            tmp.setPlz(match.getPlz());
            return new Tuple2<>(t._1(), tmp)
        });

        sc.createDataFrame(mapped.filter(e -> e._1() == "delete").flatMap((FlatMapFunction<Tuple2<String, MapNode>, MapNode>) ns
                -> Arrays.asList(ns._2()).iterator()),MapNode.class).write().parquet(con.getString("spark.plz.outputDir"));

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
}
