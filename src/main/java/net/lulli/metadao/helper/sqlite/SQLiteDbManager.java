package net.lulli.metadao.helper.sqlite;

import net.lulli.metadao.DbConnectionManager;
import net.lulli.utils.PropertiesManager;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class SQLiteDbManager extends DbConnectionManager
{

    private static SQLiteDbManager instance;
    public static final String RAMDISK_MOUNT = "/mnt/ramdisk";


    protected String JDBC_URL;
    protected String DRIVER_CLASS_NAME;
    protected String DB_USER;
    protected String DB_PASSWORD;
    static Logger log = Logger.getLogger("SQLiteDbManager");

    public Connection getConnection()
    {
        Connection con = null;
        try
        {
            Class.forName(DRIVER_CLASS_NAME);
            con = DriverManager.getConnection(JDBC_URL);
        } catch (Exception e)
        {
            log.error(e);
        }
        return con;
    }


    public void releaseConnection(Connection con)
    {
        closeConnection(con);
    }

    public void closeConnection(Connection con)
    {
        try
        {
            if (null != con)
            {
                con.close();
            }
        } catch (Exception e)
        {
            log.error(e);
        }
    }

    public void setAutocommit(Connection con, boolean autoCommit)
    {
        try
        {
            con.setAutoCommit(autoCommit);
        } catch (Exception e)
        {
            log.debug("Setting autocommit:" + e);
        }
    }

    public void commitTransaction(Connection con)
    {
        try
        {
            con.commit();
        } catch (Exception e)
        {
            log.debug("Committing transaction:" + e);
        }
    }

    public static SQLiteDbManager getInstance()
    {
        return new SQLiteDbManager();
    }

    protected void init()
    {
        //EMPTY for SQLITE
    }

    private SQLiteDbManager()
    {
        PropertiesManager configManager = PropertiesManager.getInstance();
        Properties dbProperties = configManager.getProperties();
        JDBC_URL = (String) dbProperties.get("database.sqlite.JDBC_URL");
        DRIVER_CLASS_NAME = (String) dbProperties.get("database.sqlite.DRIVER_CLASS_NAME");
        DB_USER = (String) dbProperties.get("database.sqlite.DB_USER");
        DB_PASSWORD = (String) dbProperties.get("database.sqlite.DB_PASSWORD");
    }

    public static SQLiteDbManager getInstance(String dbName)
    {

        return new SQLiteDbManager(dbName);
    }

    //private SQLiteDbManager(String dbName, boolean isRamDisk){
    private SQLiteDbManager(String dbName)
    {
        PropertiesManager configManager = PropertiesManager.getInstance();
        Properties dbProperties = configManager.getProperties();
        JDBC_URL = "jdbc:sqlite:" + dbName; //(String) dbProperties.get("etl_runquery.sqlite.JDBC_URL");
        log.debug("JDBC_URL=" + JDBC_URL);

        DRIVER_CLASS_NAME = (String) dbProperties.get("database.sqlite.DRIVER_CLASS_NAME");
        DB_USER = (String) dbProperties.get("database.sqlite.DB_USER");
        DB_PASSWORD = (String) dbProperties.get("database.sqlite.DB_PASSWORD");
    }


}
