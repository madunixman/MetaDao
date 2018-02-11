package net.lulli.metadao.helper.generic;


import net.lulli.metadao.DbConnectionManager;
import net.lulli.utils.PropertiesManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;


public class GenericDbManager extends DbConnectionManager
{
    static Logger log = Logger.getLogger("GenericDbManager");
    private static GenericDbManager instance;

    public static GenericDbManager getInstance()
    {
        if (instance == null)
        {
            instance = new GenericDbManager();
        }
        return instance;
    }

    protected void init()
    {
        Connection singleConn;
        connection_counter = 0;
        connections = new ArrayList<Connection>();
        for (int i = 0; i < pool_size; i++)
        {
            try
            {
                System.out.println("DRIVER_CLASS_NAME=" + DRIVER_CLASS_NAME);
                Class.forName(DRIVER_CLASS_NAME);
                singleConn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
                connections.add(singleConn);
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private GenericDbManager()
    {
        PropertiesManager configManager = PropertiesManager.getInstance();
        log.debug("BEFORE configManager.getProperties()");
        Properties dbProperties = configManager.getProperties();
        log.debug("AFTER configManager.getProperties()");
        JDBC_URL = (String) dbProperties.get("database.JDBC_URL");
        DRIVER_CLASS_NAME = (String) dbProperties.get("database.DRIVER_CLASS_NAME");
        DB_USER = (String) dbProperties.get("database.DB_USER");
        DB_PASSWORD = (String) dbProperties.get("database.DB_PASSWORD");
        log.debug("JDBC_URL =" + JDBC_URL);
        log.debug("DRIVER_CLASS_NAME =" + DRIVER_CLASS_NAME);
        log.debug("DB_USER =" + DB_USER);
        log.debug("DB_PASSWORD =" + DB_PASSWORD);
        init();
    }

}
