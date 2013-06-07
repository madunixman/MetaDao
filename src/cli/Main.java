package cli;


import java.sql.Connection;

import net.lulli.metadao.DbManager;

public class Main {
	
	public static void main(String[] args) {
		DbManager dbm = DbManager.getInstance();
		Connection conn = dbm.getConnection();
		if (null != conn){
			System.out.println("Connessione OK");
			dbm.releaseConnection(conn);
			System.out.println("Connessione rilasciata OK");
		} else {
			System.out.println("Connessione NULL!!!");
		}
		
	}
}
