package com.zeb.spark;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by jguenther on 09.12.2016.
 * <p>
 * Class to make the PLZ serializable in Spark
 */

@Data
@NoArgsConstructor
public class FeatureWrapper implements Serializable {

    private String plz;

    private BoundingBox bounds;

    private double lon;

    private double lat;


    public FeatureWrapper(SimpleFeature fs) {
        this.bounds = fs.getBounds();
        checkNotNull(fs.getAttribute("plz"));
        this.plz = (String) fs.getAttribute("plz");
    }


}

