package com.zeb.spark;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.geometry.BoundingBox;

import java.io.Serializable;

/**
 * Created by jguenther on 09.12.2016.
 * <p>
 * Class to make the PLZ serializable in Spark
 */

@Data
@NoArgsConstructor
public class FeatureWrapper implements Serializable {

    private Integer plz;

    private BoundingBox bounds;



    public FeatureWrapper(Integer plz, BoundingBox bx) {
        this.plz = plz;
        this.bounds = bx;
    }

    public FeatureWrapper(SimpleFeature fs) {
        this.bounds = fs.getBounds();
        this.plz = Integer.valueOf((String) fs.getAttribute("plz"));
    }



}

