package net.lulli.utils;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

public class PropertiesManager
{
    private static PropertiesManager istanza;
    public static final String APPLICATION_CONFIGURATION_FILE = "database.properties";
    Properties properties;

    static Logger log = Logger.getLogger("PropertiesManager");

    private PropertiesManager()
    {
        properties = new Properties();
        try
        {
            properties.load(new FileInputStream(APPLICATION_CONFIGURATION_FILE));
        } catch (Exception e)
        {
            log.error("Cannot load file: [" + APPLICATION_CONFIGURATION_FILE + "]");
        }
    }

    public static PropertiesManager getInstance()
    {
        if (istanza == null)
        {
            istanza = new PropertiesManager();
        }
        return istanza;
    }

    public Properties getProperties()
    {
        return properties;
    }

}

