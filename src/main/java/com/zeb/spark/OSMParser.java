package com.zeb.spark;

import com.zeb.spark.MapNode;
import lombok.Getter;
import org.geotools.geometry.Envelope2D;
import org.geotools.geometry.GeometryBuilder;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by jguenther on 12.12.2016.
 */
public class OSMParser implements Runnable {

    private final GeometryBuilder builder;
    private XMLEventReader reader;

    private String path;

    private HashMap<String, String> tags;


    private static final String MODIFY = "modify";
    private static final String DELETE = "delete";
    private static final String CREATE = "CREATE";
    private static final String NODE = "node";
    private static final String ATM = "atm";

    @Getter
    private List<MapNode> updateNodes;
    @Getter
    private List<MapNode> deleteNodes;
    @Getter
    private List<MapNode> newNodes;

    public OSMParser(InputStream is) {
        // Check
        tags = new HashMap<>();

        updateNodes = new ArrayList<>();
        deleteNodes = new ArrayList<>();
        newNodes = new ArrayList<>();
        this.builder = new GeometryBuilder(DefaultGeographicCRS.WGS84);
        try {
            reader = XMLInputFactory.newInstance().createXMLEventReader(is);
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }

    public OSMParser(String in) {
        tags = new HashMap<>();

        updateNodes = new ArrayList<>();
        deleteNodes = new ArrayList<>();
        newNodes = new ArrayList<>();
        this.builder = new GeometryBuilder(DefaultGeographicCRS.WGS84);
        try {
            reader = XMLInputFactory.newInstance().createXMLEventReader(new StringReader(in));
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }

    }

    public void clean() {
        updateNodes.clear();
        deleteNodes.clear();
        newNodes.clear();
        try {
            reader.close();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }

    public void start() throws XMLStreamException {

        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            if (event.isStartElement()) {
                StartElement start = event.asStartElement();
                if (start.getName().getLocalPart().matches("^(modify|create|delete)"))
                    handleCategory(start.getName().getLocalPart());

            }
            if (event.isEndElement()) {

            }

        }
        System.out.println("done");
    }

    private void handleCategory(String mode) throws XMLStreamException {
        while (reader.hasNext()) {
            XMLEvent event = reader.nextEvent();
            //R
            if (event.isEndElement() && event.asEndElement().getName().getLocalPart().matches("^(modify|delete|create)")) {
                return;
            }


            //Match all Nodes
            if (event.isStartElement() && event.asStartElement().getName().getLocalPart() == NODE) {
                //Map for Tags
                tags.clear();
                StartElement start = event.asStartElement();
                String nodeId = start.getAttributeByName(new QName("id")).getValue();
                String version = start.getAttributeByName(new QName("version")).getValue();
                String changeset = start.getAttributeByName(new QName("changeset")).getValue();
                String timeStamp = start.getAttributeByName(new QName("timestamp")).getValue();
                String lon = start.getAttributeByName(new QName("lon")).getValue();
                String lat = start.getAttributeByName(new QName("lat")).getValue();

                //Read Tag Elements
                while (reader.hasNext()) {
                    XMLEvent innerEL = reader.nextEvent();
                    if (innerEL.isEndElement() && innerEL.asEndElement().getName().getLocalPart() == NODE)
                        break;
                    //Tag Element
                    if (innerEL.isStartElement() && innerEL.asStartElement().getName().getLocalPart() == "tag") {
                        StartElement tag = innerEL.asStartElement();
                        String key = tag.getAttributeByName(new QName("k")).getValue();
                        String val = tag.getAttributeByName(new QName("v")).getValue();
                        tags.put(key, val);
                    }
                }
                // only append if banks or atms
                if (tags.keySet().contains("amenity") && tags.get("amenity").matches("^(bank|atm)")) {
                    MapNode ps = new MapNode();
                    // Nice casting^^
                    ps.setBounds(new Envelope2D(builder.createPoint(Double.valueOf(lon), Double.valueOf(lat)).getEnvelope()));
                    ps.setNodeId(Long.valueOf(nodeId));
                    ps.setVersion(Integer.valueOf(version));
                    ps.setChangeSetId(Long.valueOf(changeset));
                    //Tag Attributes -> can be empty
                    String plz = tags.getOrDefault("addr:postcode", null);
                    if (plz != null) {
                        ps.setPlz(Integer.valueOf(plz));
                    }
                    ps.setType(tags.get("amenity"));
                    ps.setOpeningHours(tags.getOrDefault("opening_hours", null));
                    ps.setOperator(tags.getOrDefault("operator", null));
                    ps.setName(tags.getOrDefault("name:en", tags.getOrDefault("name", null)));
                    ps.setStreetName(tags.getOrDefault("addr:street", null));
                    ps.setCountry(tags.getOrDefault("addr:country", null));
                    ps.setCity(tags.getOrDefault("addr:city", null));
                    ps.setType(mode);

                    switch (mode) {
                        case MODIFY:
                            updateNodes.add(ps);
                            break;
                        case CREATE:
                            newNodes.add(ps);
                            break;
                        case DELETE:
                            deleteNodes.add(ps);
                            break;
                    }
                }
            }
        }

    }


    @Override
    public void run() {
        try {
            this.start();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
    }
}
