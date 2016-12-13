import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.geometry.BoundingBox;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jguenther on 06.12.2016.
 */
public class matchPLZSpark {


    public static void main(String[] args) throws CQLException, IOException {
        SparkConf conf = new SparkConf().setAppName("PLZMerging").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String atmPath = "C:\\Users\\jguenther\\Downloads\\berlin-latest-free.shp\\gis.osm_pois_free_1.shp";
        //Include all ATMs and Banks
        Filter fs = ECQL.toFilter("code='2602' or code='2601'");
        JavaRDD<SimpleFeature> atms = sc.parallelize(extractFeatures(atmPath, fs));
        atms.cache();

        String pathPLZ = "C:\\Users\\jguenther\\Documents\\plz-gebiete.shp";
        //Broadcast all PLZ for Join
        Broadcast<List<SimpleFeature>> plzLs = sc.broadcast(extractFeatures(pathPLZ, Filter.INCLUDE));

        JavaPairRDD<Integer, SimpleFeature> plzATM = atms.mapToPair(e -> {
            BoundingBox cor = e.getBounds();
            SimpleFeature plzc = plzLs.value().stream().filter(ts -> ts.getBounds().contains(cor)).findFirst().get();
            Integer plz = Integer.parseInt((String) plzc.getAttribute("plz"));
            return new Tuple2<>(plz, plzc);
        });

       //Save to File
       // plzATM.saveAsHadoopFile();

        System.out.print(atms.collect().get(0).toString());


    }

    @SuppressWarnings("Duplicates")
    private static List<SimpleFeature> extractFeatures(String path, Filter filter) throws IOException {
        List<SimpleFeature> ls = new ArrayList<>();

        final Map<String, Object> map = new HashMap<>();
        map.put("url", (new File(path)).toURI().toURL());
        DataStore dataStore = DataStoreFinder.getDataStore(map);
        final String typeName = dataStore.getTypeNames()[0];

        FeatureSource<SimpleFeatureType, SimpleFeature> source = dataStore
                .getFeatureSource(typeName);
        FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures(filter);
        try (FeatureIterator<SimpleFeature> features = collection.features()) {
            while (features.hasNext()) {
                features.next().getBounds().toString();

                ls.add(features.next());
            }
            features.close();
        }
        dataStore.dispose();
        return ls;
    }


}
