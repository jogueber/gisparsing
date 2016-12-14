package com.zeb.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by jguenther on 12.12.2016.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MapNode extends FeatureWrapper {

    private long nodeId;

    private long changeSetId;

    private int version;

    public String timeStamp;

    private String streetName;

    private String city;

    private String country;

    private String openingHours;

    private String name;

    private String operator;

    private String type;

}
