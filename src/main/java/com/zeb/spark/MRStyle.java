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
import java.net.URL;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by jguenther on 13.12.2016.
 */
public class MRStyle {

    public static Logger logger;

    public static void main(String[] args) {


        logger = LogManager.getLogger("MRStyle");

        Config con = ConfigFactory.load("application.conf");

        URL file = MRStyle.class.getClassLoader().getResource("plz-gebiete.shp");
        checkNotNull(file);
        List<FeatureWrapper> feat = null;
        try {
            feat = extractFeatures(file, Filter.INCLUDE);
        } catch (IOException e) {
            e.printStackTrace();
        }


        SparkConf conf = new SparkConf().setAppName("plzmerging");


        JavaSparkContext jc = new JavaSparkContext(conf);
        logger.info("Context successfully initialized");


        Broadcast<List<FeatureWrapper>> plzLs = jc.broadcast(feat);
        logger.info("Successfully read PLZ");

        Broadcast<List<String>> key = jc.broadcast(con.getStringList("spark.plz.keys"));



        // support wildcards
        JavaPairRDD<String, MapNode> ze = jc.wholeTextFiles(con.getString("spark.plz.inputDir")).flatMapToPair(t -> {
            OSMParser os = new OSMParser(t._2(), key.getValue());
            os.start();
            List<Tuple2<String, MapNode>> results = new ArrayList<>();
            os.getUpdateNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getDataType(), e)));
            os.getNewNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getDataType(), e)));
            os.getDeleteNodes().stream().forEach(e -> results.add(new Tuple2<>(e.getDataType(), e)));
            return results;
        });


        final JavaPairRDD<String, MapNode> mapped = ze.mapToPair(t -> {
            Optional<FeatureWrapper> match = plzLs.value().stream().filter(e -> e.getBounds().contains(t._2().getBounds())).findFirst();
            match.ifPresent(e -> t._2().setPlz(e.getPlz()));
            return t;
        });


        final JavaRDD<MapNode> newNodes = mapped.filter(e -> e._1() == "create").flatMap(ns
                -> Lists.newArrayList(ns._2()));


        final JavaRDD<Row> converted = mapped.map(el -> {
            MapNode ns = el._2();
            Long plz = ns.getPlz() == null ? 0L : Long.valueOf(ns.getPlz());

            return RowFactory.create(ns.getStreetName(), ns.getCity(), ns.getCountry(), ns.getOpeningHours(),
                    ns.getName(), ns.getOperator(), ns.getDataType(), ns.getNodeType(), ns.getNodeId(), ns.getChangeSetId(), Long.valueOf(String.valueOf(ns.getVersion())),
                    plz, ns.getLon(), ns.getLat(), ns.getTimeStamp());
        });

        // Write everything in one file
        // For Version 1.6.0
        final SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jc);

        sqlContext.createDataFrame(converted, getSchema()).write().parquet(con.getString("spark.plz.outputDir") + "/extracts" + System.currentTimeMillis() + ".parquet");


        deleteFiles(con.getString("spark.plz.inputDir"));

        jc.stop();

    }

    @SuppressWarnings("Duplicates")
    public static List<FeatureWrapper> extractFeatures(String path, Filter filter) throws IOException {
        List<FeatureWrapper> ls = new ArrayList<>();
/*
        Configuration conf = new Configuration();
        FileSystem fS = FileSystem.get(conf);

        String s = "/tmp/plz";
        fS.copyToLocalFile(false, new Path(path), new Path(new File(s).toURI()));*/
        final Map<String, Object> map = new HashMap<>();
        //todo -> check if it breaks with HDFS
        logger.info("File Path:" + (new File(path)).toURI().toURL());
        map.put("url", (new File(path)).toURI().toURL());
        logger.info("Map Size:" + map.size());
        logger.info("Available DS:" + DataStoreFinder.getAllDataStores());
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

    @SuppressWarnings("Duplicates")
    public static List<FeatureWrapper> extractFeatures(URL path, Filter filter) throws IOException {
        List<FeatureWrapper> ls = new ArrayList<>();
/*
        Configuration conf = new Configuration();
        FileSystem fS = FileSystem.get(conf);

        String s = "/tmp/plz";
        fS.copyToLocalFile(false, new Path(path), new Path(new File(s).toURI()));*/
        final Map<String, Object> map = new HashMap<>();
        //todo -> check if it breaks with HDFS

        map.put("url", path);
        //   logger.info("Map Size:" +map.get);
        //   logger.info("Available DS:" + DataStoreFinder.getAllDataStores());
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
        String schemaString = "timeStamp streetName city country openingHours name operator datatype nodeType";
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
        schemaString = "lon lat";
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.DoubleType, true);
            fields.add(field);
        }
        schemaString = "timestamp";
        StructField field = DataTypes.createStructField(schemaString, DataTypes.TimestampType, true);
        fields.add(field);

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
            logger.error("Error deleting files!");
        }
    }
}
