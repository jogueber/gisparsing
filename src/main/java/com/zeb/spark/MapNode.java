package com.zeb.spark;

import lombok.*;

import java.time.Instant;

/**
 * Created by jguenther on 12.12.2016.
 * <p>
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

    private String dateAsString;

    /**
     * Sets the type of record ->CREATE, Update, Delete
     */
    private String dataType;
    /**
     * Sets either amenity or shoptype
     */
    private String nodeType;


    private boolean wheelchair;

}
