package net.lulli.metadao;

import net.lulli.utils.PropertiesManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Properties;

public class DbManager extends DbConnectionManager
{

    private static DbManager instance;

    public static DbManager getInstance()
    {
        if (instance == null)
        {
            instance = new DbManager();
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
                Class.forName(DRIVER_CLASS_NAME);
                singleConn = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
                connections.add(singleConn);
            } catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }

    private DbManager(String databaseName)
    {
        PropertiesManager configManager = PropertiesManager.getInstance();
        Properties dbProperties = configManager.getProperties();
        JDBC_URL = (String) dbProperties.get(databaseName + ".JDBC_URL");
        DRIVER_CLASS_NAME = (String) dbProperties.get(databaseName + ".DRIVER_CLASS_NAME");
        DB_USER = (String) dbProperties.get(databaseName + ".DB_USER");
        DB_PASSWORD = (String) dbProperties.get(databaseName + ".DB_PASSWORD");
        init();
    }

    private DbManager()
    {
        PropertiesManager configManager = PropertiesManager.getInstance();
        Properties dbProperties = configManager.getProperties();
        JDBC_URL = (String) dbProperties.get("database.JDBC_URL");
        DRIVER_CLASS_NAME = (String) dbProperties.get("database.DRIVER_CLASS_NAME");
        DB_USER = (String) dbProperties.get("database.DB_USER");
        DB_PASSWORD = (String) dbProperties.get("database.DB_PASSWORD");
        init();
    }

}
