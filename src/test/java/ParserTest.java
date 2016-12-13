import com.google.common.io.Resources;
import org.apache.parquet.filter.ColumnPredicates;
import org.junit.Before;
import org.junit.Test;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;


import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


/**
 * Created by jguenther on 12.12.2016.
 */

public class ParserTest {


    @Test
    public void assertFind() throws IOException, URISyntaxException {

        InputStream in = Files.newInputStream(new File(this.getClass().getClassLoader().getResource("251.osc").toURI()).toPath(), StandardOpenOption.READ);
        OSMParser parser = new OSMParser(in);
        try {
            parser.start();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        assertThat("update", parser.getUpdateNodes().isEmpty(), equalTo(false));
        assertThat("delete", parser.getDeleteNodes().isEmpty(), equalTo(false));

        assertThat("Number less than number of banks+atms", parser.getDeleteNodes().size() + parser.getNewNodes().size() + parser.getUpdateNodes().size(),
                lessThanOrEqualTo(282 + 51));


    }


}
