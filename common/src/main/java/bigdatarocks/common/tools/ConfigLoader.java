package bigdatarocks.common.tools;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {

    private static Logger LOGGER = Logger.getLogger(ConfigLoader.class);

    public static Properties loadProperties() throws Exception{

        Properties properties = new Properties();
        InputStream input = null;
        try {
            input = new FileInputStream("config.properties");
            properties.load(input);
        } catch (IOException e) {
            LOGGER.error("Failed to load configuration of the application");
            throw e;
        }
        return properties;
    }
}
