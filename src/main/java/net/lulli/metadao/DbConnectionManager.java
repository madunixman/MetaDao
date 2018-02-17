package net.lulli.metadao;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public abstract class DbConnectionManager
{
    static Logger log = Logger.getLogger("DbConnectionManager");
    protected String JDBC_URL;
    protected String DRIVER_CLASS_NAME;
    protected String DB_USER;
    protected String DB_PASSWORD;

//	public  Connection getConnection(){
//		 Connection con = null;
//		    try{
//		      Class.forName(DRIVER_CLASS_NAME);
//		      System.out.println("DRIVER_CLASS_NAME =["+DRIVER_CLASS_NAME +"], JDBC_URL=[" +JDBC_URL+"], DB_USER=["+DB_USER+"], DB_PASSWORD=["+DB_PASSWORD+"]");
//		      con = DriverManager.getConnection(JDBC_URL,DB_USER,DB_PASSWORD);
//		    }
//		    catch (Exception e){
//		    	e.printStackTrace();
//		    }
//		    return con;
//	}
//	
//	public  void closeConnection( Connection con){
//		    try{
//		    	if (null != con){
//		    		con.close();
//		    	} 
//		    }
//		    catch (Exception e){
//		      e.printStackTrace();
//		    }
//	}

    protected List<Connection> connections;
    protected int pool_size = 3;
    protected int connection_counter;


    public Connection getConnection()
    {
        Connection con = null;
        connection_counter++;
        con = connections.get(connection_counter % pool_size);
        return con;
    }

    public Connection OLDgetConnection()
    {
        Connection con = null;
        try
        {
            Class.forName(DRIVER_CLASS_NAME);
            log.debug("DRIVER_CLASS_NAME =[" + DRIVER_CLASS_NAME + "], JDBC_URL=[" + JDBC_URL + "], DB_USER=[" + DB_USER + "], DB_PASSWORD=[" + DB_PASSWORD + "]");
            if (con == null)
            {
                con = DriverManager.getConnection(JDBC_URL, DB_USER, DB_PASSWORD);
            }
        } catch (Exception e)
        {
            log.error(e);
        }
        return con;
    }

    public void releaseConnection(Connection con)
    {
//		    try{
//		    	if (null != con){
//		    		con.close();
//		    	} 
//		    }
//		    catch (Exception e){
//		      e.printStackTrace();
//		    }
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


}

