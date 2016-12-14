package com.zeb.spark;

import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Created by jguenther on 13.12.2016.
 */
public class MRStyle {

    public static void main(String[] args) {

        Config con = ConfigFactory.load("application.conf");

        Logger ls = LogManager.getLogger("MRStyle");

        final String s = con.getString("spark.app");


        SparkConf conf = new SparkConf().setAppName(s);
        String warehouse = con.getString("spark.sql.warehouse.dir");
        if (warehouse != null || !warehouse.isEmpty()) {
            conf.set("spark.sql.warehouse.dir", warehouse);
        }


        JavaSparkContext jc = new JavaSparkContext(conf);
        ls.info("Context successfully initialized");

        Broadcast<List<FeatureWrapper>> plzLs = null;
        try {
            plzLs = jc.broadcast(extractFeatures(con.getString("spark.plz.inputPLZ"), Filter.INCLUDE));
            ls.info("Successfully read PLZ");
        } catch (IOException e) {
            e.printStackTrace();
            ls.error("Error Reading PLZ");
        }


        // support wildcards
        JavaPairRDD<String, MapNode> ze = jc.wholeTextFiles(con.getString("spark.plz.inputDir")).flatMapToPair(t -> {
            OSMParser os = new OSMParser(t._2());
            os.start();
            List<Tuple2<String, MapNode>> results = new ArrayList<>();
            os.getUpdateNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getType(), e)));
            os.getNewNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getType(), e)));
            os.getDeleteNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getType(), e)));
            return results;
        });

        final Broadcast<List<FeatureWrapper>> finalPlzLs = plzLs;
        final JavaPairRDD<String, MapNode> mapped = ze.mapToPair(t -> {
            Optional<FeatureWrapper> match = finalPlzLs.value().stream().filter(e -> e.getBounds().contains(t._2().getBounds())).findFirst();
            match.ifPresent(e -> t._2().setPlz(e.getPlz()));
            return t;
        });


        final JavaRDD<MapNode> newNodes = mapped.filter(e -> e._1() == "create").flatMap(ns
                -> Lists.newArrayList(ns._2()));


        final JavaRDD<Row> converted = mapped.map(el -> {
            MapNode ns = el._2();
            return RowFactory.create(ns.getTimeStamp(), ns.getStreetName(), ns.getCity(), ns.getCountry(), ns.getOpeningHours(),
                    ns.getName(), ns.getOperator(), ns.getType(), ns.getNodeId(), ns.getChangeSetId(), ns.getVersion(),
                    ns.getPlz());
        });

        // Write everything in one file
        // For Version 1.6.0
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jc);

        sqlContext.createDataFrame(converted, getSchema()).write().parquet(con.getString("spark.plz.outputDir") + "\\extracts" + LocalDateTime.now().format(DateTimeFormatter.BASIC_ISO_DATE) + ".parquet");


        deleteFiles(con.getString("spark.plz.inputDir"));

    }

    @SuppressWarnings("Duplicates")
    public static List<FeatureWrapper> extractFeatures(String path, Filter filter) throws IOException {
        List<FeatureWrapper> ls = new ArrayList<>();

        Configuration conf = new Configuration();
        FileSystem fS = FileSystem.get(conf);

        String s = "/tmp/plz";
        fS.copyToLocalFile(false, new Path(path), new Path(new File(s).toURI()));
        final Map<String, Object> map = new HashMap<>();
        //todo -> check if it breaks with HDFS
        map.put("url", (new File(s)).toURI().toURL());
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

    //Notwendig weil er die Bounding box nicht mag
    public static StructType getSchema() {
        String schemaString = "timeStamp streetName city country openingHours name operator type";
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        schemaString = "nodeId changeSetId version plz";
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.LongType, true);
            fields.add(field);
        }
        return DataTypes.createStructType(fields);
    }

    public static void deleteFiles(String path) {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            fs.delete(new Path(path), true);
        } catch (IOException e) {
            e.printStackTrace();
            e.initCause(new IOException("Error Deleting files!"));
        }
    }
}
