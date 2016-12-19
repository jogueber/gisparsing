package com.zeb.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Created by jguenther on 12.12.2016.
 *
 * Datatype
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MapNode extends FeatureWrapper {

    private long nodeId;

    private long changeSetId;

    private long version;

    public Instant timeStamp;

    private String streetName;

    private String city;

    private String country;

    private String openingHours;

    private String name;

    private String operator;

    //UPDATE,DELETE, CREATE
    private String dataType;

    private String nodeType;

}
