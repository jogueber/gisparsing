import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.zeb.spark.OSMParser;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;


import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


/**
 * Created by jguenther on 12.12.2016.
 */

public class ParserTest {

    @Test
    public void testSetup() {
        Config con = ConfigFactory.load("application.conf");
        // check Keys
        assertThat("Null value for keys", con.getStringList("spark.plz.keys"), allOf(notNullValue(), not(empty())));
        // check configuration
        assertThat("Output Dir not set; Please ensure spark.plz.outputDir is set in application.conf", con.getString("spark.plz.outputDir"),
                allOf(notNullValue()));

    }


    @Test
    public void assertFind() throws IOException, URISyntaxException {
        Config con = ConfigFactory.load("application.conf");

        InputStream in = Files.newInputStream(new File(this.getClass().getClassLoader().getResource("251.osc").toURI()).toPath(), StandardOpenOption.READ);
        OSMParser parser = new OSMParser(in, con.getStringList("spark.plz.keys"));
        try {
            parser.start();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        assertThat("Update Nodes are empty; check if the test data contains updated data that should be in the result set", parser.getUpdateNodes(), not(empty()));
        assertThat("Deleted Nodes are empty; Ensure that the test data contains not deleted nodes that should be parsed", parser.getDeleteNodes(), not(empty()));
        assertThat("New nodes are empty,ensure that the test data does not contain any new nodes that should be parsed", parser.getNewNodes(), not(empty()));


    }


}
