import com.google.common.collect.ArrayListMultimap;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.Query;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created by jguenther on 05.12.2016.
 */
public class matchPLZ {

    public static void main(String[] args) throws IOException, CQLException {
        LocalDateTime start = LocalDateTime.now();
        //Extract ATM's
        String atmPath = "C:\\Users\\jguenther\\Downloads\\berlin-latest-free.shp\\gis.osm_pois_free_1.shp";
        Filter fs = ECQL.toFilter("code='2602'");
        List<SimpleFeature> atms = extractFeatures(atmPath, fs);

        //Extract Banks
        Filter bankFS = ECQL.toFilter("code='2601'");
        List<SimpleFeature> banks = extractFeatures(atmPath, bankFS);
        atms.addAll(banks);

        //Extract
        String plzPath = "C:\\Users\\jguenther\\Documents\\plz-gebiete.shp";
        Filter incl = Filter.INCLUDE;
        List<SimpleFeature> plz = extractFeatures(plzPath, incl);
        //merging
        ArrayListMultimap<String, SimpleFeature> ma = ArrayListMultimap.create();
        for (SimpleFeature s : atms) {
            plz.parallelStream().filter(plzel -> plzel.getBounds().contains(s.getBounds())).findFirst().ifPresent(ples -> {
                String splz = (String) ples.getAttribute("plz");
                ma.put(splz, s);
            });
        }
        atms = null;
        ma.keySet().stream().forEach(plza -> System.out.println("Results contain PLZ:" + plza + " with " + ma.get(plza).size() + " ATMS"));



        System.out.println("Runtime: "+ Duration.between(start,LocalDateTime.now()).getSeconds()+" Seconds");
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
                ls.add(features.next());
            }
            features.close();
        }
        dataStore.dispose();
        return ls;

    }
}
