package net.lulli.metadao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import net.lulli.metadao.model.Dao;
import net.lulli.metadao.model.MetaDto;
import net.lulli.metadao.model.SQLDialect;

import org.apache.log4j.Logger;


public class MetaDao extends Dao{
static Logger log =  Logger.getLogger("MetaDao");
	
	public MetaDao(){
		//dbConnectionmanager  = DbManager.getInstance();
	}	
	
	public MetaDao(String tableName){
		this.TABLE_NAME = tableName;
		//dbConnectionmanager  = DbManager.getInstance();
	}	
	
	public String insert(MetaDto dto, Connection conn){
		this.TABLE_NAME = dto.getTableName();
		log.debug("BEGIN insert()");
		String uuid = null;
		
		///////////
		if (dto.containsKey(null)){
			dto.remove(null);
			log.debug("METADAO_INSERT: REMOVE_NULL");
		}
		//////////
		
		Set<String> keys = dto.keySet();
		Iterator<String> keysIterator = keys.iterator();
		Iterator<String> keysIteratorSymbols = keys.iterator();
		//String primaryKey = dto.getIdField();
		
	
		PreparedStatement pstmt = null;
		String k;
		boolean isFirstField = true;
		String sql = "INSERT INTO " + TABLE_NAME + " " +
				"(" ;
				while(keysIterator.hasNext()){
					k = keysIterator.next();
					if (isFirstField){
						sql = sql +  " " + k;
						isFirstField = false;
					} else {
						sql = sql +  ", " + k;
					}
				}
			
				if (dto.isChronology()){
					sql = sql +
							",updated,"+
							"created";
				} 
				sql = sql +") values ( ";
				
				boolean isFirstPlaceHolder = true;
				while(keysIteratorSymbols.hasNext()){
					k = keysIteratorSymbols.next();
					if (isFirstPlaceHolder){
						sql = sql + "? ";
						isFirstPlaceHolder = false;
					} else {
						sql = sql + ",? ";
					}
				}
				if (dto.isChronology()){
					sql = sql+
					",now(), now()";
				}
				sql = sql+
				")" ;
		log.debug("SQL=["+ sql +"]");
		try{
			//conn  = dbConnectionmanager.getConnection();
			log.debug("BEFORE prepareStatement()");
			pstmt = conn.prepareStatement(sql);			
			log.debug("AFTER prepareStatement()");	
			//String pkValue ="";
					//int idx = 2;
			int idx = 1;
			Iterator<String> keysIterator2 = keys.iterator();
			String key;
			String value ="";
			while(keysIterator2.hasNext()){
				 key = keysIterator2.next() ;
					 Object kvalue =  dto.get(key);
					 if ( null != kvalue){
						 value = kvalue.toString();
					 }
					 pstmt.setString(idx,   value);
					 log.trace("Adding key: [" + key +"] with index: ["+idx+"] with value: + [" + value + "]"); 
					 idx++;	
			}
			log.debug("BEFORE execute()");
			pstmt.execute();
			log.debug("AFTER execute()");
		} catch (Exception e) {
			log.error("Eccezione:" + e);
		} finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e);
			}
		}
		return uuid;
	}
	
	
	
	public void update(MetaDto dto, Hashtable wheres, Connection conn){
		this.TABLE_NAME = dto.getTableName();
		log.debug("BEGIN insert()");
		MetaDto requestDto = (MetaDto)dto;
		//String uuid = clienteDto.getId();
		String primaryKey = dto.getIdField();
		//String pKeyValue = dto.getId();
		log.debug("BEGIN insert()");
		//Connection conn = null; 
		PreparedStatement pstmt = null;
		Set<String> keys = dto.keySet();
		Iterator<String> keysIterator_set = keys.iterator();
		
		boolean isFirst = true;
		String sql = "UPDATE " + TABLE_NAME + " set " ;
		//+ " profile =? ";
		//+ " ,variableName =? "
		int index = 0;
		String k;
		while (keysIterator_set.hasNext()){
			k = keysIterator_set.next();
			if ( ! k.equals(primaryKey) ){
				//pstmt.setString(index, (String)requestDto.get(k));
				if (isFirst){
					sql = sql +" "+ k +" =? "; // Inizia senza la virgola
					isFirst = false;
				} else {
					sql = sql +", "+ k +" =? ";// Inizia con la virgola
				}
			}
			index++;
		}
		if (dto.isChronology()){
			sql = sql + ", updated = now() ";
		}
		index = 1;
		Set<String> whereKeysP1 = wheres.keySet();
		Iterator<String> wheresIteratorP1 = whereKeysP1.iterator();
		String whereKey;
		String whereValue;
		boolean whereIteratorIsFirstP1 = true;
		while (wheresIteratorP1.hasNext()){
			whereKey = wheresIteratorP1.next();
			whereValue = wheres.get(whereKey).toString();
			if (whereIteratorIsFirstP1){
				sql = sql+ " where "+ whereKey +" = ? ";
				whereIteratorIsFirstP1 = false;
			} else {
				sql = sql+ "  AND "+ whereKey +" = ? ";
			}
		}
		log.debug("SQL=["+ sql +"]");
		try{
			log.debug("BEFORE prepareStatement()");
			pstmt = conn.prepareStatement(sql);		
			Iterator<String> keysIterator_parameter = keys.iterator();
			while (keysIterator_parameter.hasNext()){
				k = keysIterator_parameter.next();
				//if ( ! k.equals(primaryKey) ){
					pstmt.setString(index, (String)requestDto.get(k));
					index++;
				//}
			}
			Set<String> whereKeysP2 = wheres.keySet();
			Iterator<String> wheresIteratorP2 = whereKeysP2.iterator();

			while (wheresIteratorP2.hasNext()){
				whereKey = wheresIteratorP2.next();
				whereValue = wheres.get(whereKey).toString();
				pstmt.setString(index, (String)wheres.get(whereKey));
				index++;
			}
				
			//pstmt.setString(index, pKeyValue);
			log.debug("AFTER prepareStatement()");			
			log.debug("BEFORE execute()");
			pstmt.execute();
			log.debug("AFTER execute()");
		} catch (Exception e) {
			log.error("" + e);
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		//return pKeyValue;
	}
	
	
	
	public void delete(MetaDto dto, Hashtable wheres, Connection conn){
		this.TABLE_NAME = dto.getTableName();
		log.debug("BEGIN insert()");
		MetaDto requestDto = (MetaDto)dto;
		//String uuid = clienteDto.getId();
		String primaryKey = dto.getIdField();
		//String pKeyValue = dto.getId();
		log.debug("BEGIN insert()");
		//Connection conn = null; 
		PreparedStatement pstmt = null;
		Set<String> keys = dto.keySet();
		Iterator<String> keysIterator_set = keys.iterator();
		
		String sql = "DELETE FROM " + TABLE_NAME + " " ;
		int index = 0;
		String k;
		index = 1;
		Set<String> whereKeysP1 = wheres.keySet();
		Iterator<String> wheresIteratorP1 = whereKeysP1.iterator();
		String whereKey;
		String whereValue;
		boolean whereIteratorIsFirstP1 = true;
		while (wheresIteratorP1.hasNext()){
			whereKey = wheresIteratorP1.next();
			whereValue = wheres.get(whereKey).toString();
			if (whereIteratorIsFirstP1){
				sql = sql+ " where "+ whereKey +" = ? ";
				whereIteratorIsFirstP1 = false;
			} else {
				sql = sql+ "  AND "+ whereKey +" = ? ";
			}
		}
		log.debug("SQL=["+ sql +"]");
		try{
			log.debug("BEFORE prepareStatement()");
			pstmt = conn.prepareStatement(sql);		
			Iterator<String> keysIterator_parameter = keys.iterator();
			while (keysIterator_parameter.hasNext()){
				k = keysIterator_parameter.next();
				pstmt.setString(index, (String)requestDto.get(k));
				index++;
			}
			Set<String> whereKeysP2 = wheres.keySet();
			Iterator<String> wheresIteratorP2 = whereKeysP2.iterator();

			while (wheresIteratorP2.hasNext()){
				whereKey = wheresIteratorP2.next();
				whereValue = wheres.get(whereKey).toString();
				pstmt.setString(index, (String)wheres.get(whereKey));
				index++;
			}
			log.debug("AFTER prepareStatement()");			
			log.debug("BEFORE execute()");
			pstmt.execute();
			log.debug("AFTER execute()");
		} catch (Exception e) {
			log.error("" + e);
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
	}
	
	
	public List searchSybaseTop1( MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes, Integer resultRows ){	
		List listOfDto = null;
		String sqlDialect = this.SQL_DIALECT;
		try{
				listOfDto = searchTop1Sybase( requestDto,  wheres,  conn,  definedAttributes,  resultRows );
		} catch (Exception e) {
			log.error(e);
		}
		return listOfDto;
	}
	
	public List search( MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes, Integer resultRows ){	
		List listOfDto = null;
		String sqlDialect = this.SQL_DIALECT;
		log.debug("BEGIN search()");
		try{
			if(sqlDialect.equals(SQLDialect.STANDARD)){
				listOfDto = searchMySQL( requestDto,  wheres,  conn,  definedAttributes,  resultRows );
			} else if (sqlDialect.equals(SQLDialect.MYSQL)){
				listOfDto = searchMySQL( requestDto,  wheres,  conn,  definedAttributes,  resultRows );
			} else if (sqlDialect.equals(SQLDialect.MSSQL)){
					listOfDto = searchSybase( requestDto,  wheres,  conn,  definedAttributes,  resultRows );
			} else if (sqlDialect.equals(SQLDialect.SYBASE)){
				log.debug("ENTERING SYBASE");
				listOfDto = searchSybase( requestDto,  wheres,  conn,  definedAttributes,  resultRows );
			} 
			log.debug("END search()");
		} catch (Exception e) {
			log.error(e);
		}
		return listOfDto;
	}
	
	
	
	
	
	private List searchMySQL(MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes, Integer resultRows ){	
		this.TABLE_NAME = requestDto.getTableName();
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		try{
			//conn  = dbConnectionmanager.getConnection();
			String sqlString ="SELECT * FROM " + TABLE_NAME;
			int idx=0;
			if (null != wheres ){
				sqlString+= " WHERE ";
				Enumeration whereFields = wheres.keys(); 
				//WAS_OK::int idx=0;
				while(whereFields.hasMoreElements()){
					if (idx > 0){
						sqlString += "AND ";
					} else {
						sqlString += " ";
					}
					idx++;
					String where = (whereFields.nextElement()).toString();
					if ( null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
						sqlString += " " + where + " is null ";
					} else {
						sqlString += " " + where;
						sqlString += " = ? ";
					}
				}
			}
			if ( null != resultRows){
				if (null == resultRows){
					log.debug("NO LIMITS");
				} else {
					sqlString += " limit " +resultRows.toString();
				}
			}
			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			int paramIdx = 1;
			String whereValue;
			
			if (null != wheres ){
				Enumeration whereFields_2ndRound = wheres.keys();
				while(whereFields_2ndRound.hasMoreElements()){
					try{
						String where = whereFields_2ndRound.nextElement().toString();
						if (null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
							log.debug("skipping null value for field: [" + where +"]");
						} else {
							whereValue = wheres.get(where).toString();
							pstmt.setString(paramIdx, whereValue);
							paramIdx++;
						}
					}catch (Exception e) {
						log.error("Could not set where condition");
				}
				}
			}
			rs = pstmt.executeQuery();
		
			while(rs.next()){
		 		log.debug("rs.next()");
		 		responseDto  = new MetaDto();
		 		Set<String> keys;
		 		if (definedAttributes){
		 			// requestDto contiene i nomi delle colonne da stampare
		 			 keys = requestDto.keySet();
		 		} else {
		 			ResultSetMetaData md = rs.getMetaData() ;
		 			keys = new TreeSet<String>(); 
					for( int i = 1; i <= md.getColumnCount(); i++ ){
						keys.add(md.getColumnLabel(i));
						//log.debug("FIELD_NAME:[" +md.getColumnLabel(i) +"]");
					}
		 		}

				Iterator<String> keysIterator2 = keys.iterator();
				String key;
				String value;
				while(keysIterator2.hasNext()){
					 key = keysIterator2.next() ;
					 value = rs.getString(key);
					 responseDto.put(key, rs.getString(key));
				     //pstmt.setString(idx,   value);
					 log.debug( "["+key +"]=[" + value + "]"); 
					 idx++;	
				}
				listOfDto.add(responseDto);
		 	}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return listOfDto;
	}
	
	private List searchSybase(MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes, Integer resultRows ){	
		this.TABLE_NAME = requestDto.getTableName();
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		try{
			//conn  = dbConnectionmanager.getConnection();
			String sqlString ="SELECT ";
			if ( null != resultRows){
				if (null == resultRows){
					log.debug("NO LIMITS");
				} else {
					sqlString += " top " +resultRows.toString();
				}
			}
			sqlString+=" * FROM " + TABLE_NAME;
			int idx=0;
			if (null != wheres ){
				sqlString+= " WHERE ";
				Enumeration whereFields = wheres.keys(); 
				//WAS_OK::int idx=0;
				while(whereFields.hasMoreElements()){
					if (idx > 0){
						sqlString += "AND ";
					} else {
						sqlString += " ";
					}
					idx++;
					String where = (whereFields.nextElement()).toString();
					if ( null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
						sqlString += " " + where + " is null ";
					} else {
						sqlString += " " + where;
						sqlString += " = ? ";
					}
				}
			}
			/*
			if ( null != resultRows){
				if (null == resultRows){
					log.debug("NO LIMITS");
				} else {
					sqlString += " limit " +resultRows.toString();
				}
			}
			*/
			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			int paramIdx = 1;
			String whereValue;
			
			if (null != wheres ){
				Enumeration whereFields_2ndRound = wheres.keys();
				while(whereFields_2ndRound.hasMoreElements()){
					try{
						String where = whereFields_2ndRound.nextElement().toString();
						if (null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
							log.debug("skipping null value for field: [" + where +"]");
						} else {
							whereValue = wheres.get(where).toString();
							pstmt.setString(paramIdx, whereValue);
							paramIdx++;
						}
					}catch (Exception e) {
						log.error("Could not set where condition");
				}
				}
			}
			rs = pstmt.executeQuery();
		
			while(rs.next()){
		 		log.debug("rs.next()");
		 		responseDto  = new MetaDto();
		 		Set<String> keys;
		 		if (definedAttributes){
		 			// requestDto contiene i nomi delle colonne da stampare
		 			 keys = requestDto.keySet();
		 		} else {
		 			ResultSetMetaData md = rs.getMetaData() ;
		 			keys = new TreeSet<String>(); 
					for( int i = 1; i <= md.getColumnCount(); i++ ){
						keys.add(md.getColumnLabel(i));
						//log.debug("FIELD_NAME:[" +md.getColumnLabel(i) +"]");
					}
		 		}

				Iterator<String> keysIterator2 = keys.iterator();
				String key;
				String value;
				while(keysIterator2.hasNext()){
					 key = keysIterator2.next() ;
					 value = rs.getString(key);
					 responseDto.put(key, rs.getString(key));
				     //pstmt.setString(idx,   value);
					 log.debug( "["+key +"]=[" + value + "]"); 
					 idx++;	
				}
				listOfDto.add(responseDto);
		 	}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return listOfDto;
	}
	
	
	private List searchTop1Sybase(MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes, Integer resultRows ){	
		this.TABLE_NAME = requestDto.getTableName();
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		try{
			String sqlString ="SELECT top 1 * FROM " + TABLE_NAME;
			int idx=0;
			if (null != wheres ){
				sqlString+= " WHERE ";
				Enumeration whereFields = wheres.keys(); 
				//WAS_OK::int idx=0;
				while(whereFields.hasMoreElements()){
					if (idx > 0){
						sqlString += "AND ";
					} else {
						sqlString += " ";
					}
					idx++;
					String where = (whereFields.nextElement()).toString();
					if ( null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
						sqlString += " " + where + " is null ";
					} else {
						sqlString += " " + where;
						sqlString += " = ? ";
					}
				}
			}
			if ( null != resultRows){
				if (null == resultRows){
					log.debug("NO LIMITS");
				} else {
					sqlString += " limit " +resultRows.toString();
				}
			}
			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			int paramIdx = 1;
			String whereValue;
			
			if (null != wheres ){
				Enumeration whereFields_2ndRound = wheres.keys();
				while(whereFields_2ndRound.hasMoreElements()){
					try{
						String where = whereFields_2ndRound.nextElement().toString();
						if (null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
							log.debug("skipping null value for field: [" + where +"]");
						} else {
							whereValue = wheres.get(where).toString();
							pstmt.setString(paramIdx, whereValue);
							paramIdx++;
						}
					}catch (Exception e) {
						log.error("Could not set where condition");
				}
				}
			}
			rs = pstmt.executeQuery();
		
			while(rs.next()){
		 		log.debug("rs.next()");
		 		responseDto  = new MetaDto();
		 		Set<String> keys;
		 		if (definedAttributes){
		 			// requestDto contiene i nomi delle colonne da stampare
		 			 keys = requestDto.keySet();
		 		} else {
		 			ResultSetMetaData md = rs.getMetaData() ;
		 			keys = new TreeSet<String>(); 
					for( int i = 1; i <= md.getColumnCount(); i++ ){
						keys.add(md.getColumnLabel(i));
						//log.debug("FIELD_NAME:[" +md.getColumnLabel(i) +"]");
					}
		 		}

				Iterator<String> keysIterator2 = keys.iterator();
				String key;
				String value;
				while(keysIterator2.hasNext()){
					 key = keysIterator2.next() ;
					 value = rs.getString(key);
					 responseDto.put(key, rs.getString(key));
				     //pstmt.setString(idx,   value);
					 log.debug( "["+key +"]=[" + value + "]"); 
					 idx++;	
				}
				listOfDto.add(responseDto);
		 	}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return listOfDto;
	}
	
	
	
	public MetaDto descTable(String tableName, Connection conn ){	
		this.TABLE_NAME = tableName;
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		try{
			//conn  = dbConnectionmanager.getConnection();
			String sqlString ="select * from " + TABLE_NAME + " limit 1";
			int idx=0;

			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			rs = pstmt.executeQuery();
		
			while(rs.next()){
		 		log.debug("rs.next()");
		 		responseDto  = new MetaDto();
		 		Set<String> keys;
		 		
		 		ResultSetMetaData md = rs.getMetaData() ;
		 		keys = new TreeSet<String>(); 
				for( int i = 1; i <= md.getColumnCount(); i++ ){
					keys.add(md.getColumnLabel(i));
						//log.debug("FIELD_NAME:[" +md.getColumnLabel(i) +"]");
				}
				Iterator<String> keysIterator2 = keys.iterator();
				String key;
				String value;
				while(keysIterator2.hasNext()){
					 key = keysIterator2.next() ;
					 value = rs.getString(key);
					 responseDto.put(key, rs.getString(key));
				     //pstmt.setString(idx,   value);
					 log.debug( "["+key +"]=[" + value + "]"); 
					 idx++;	
				}
				//listOfDto.add(responseDto);
		 	}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return responseDto;
	}
	
	
	
	
	public List select(MetaDto requestDto, Hashtable wheres, boolean definedAttributes, Connection conn){	
		this.TABLE_NAME = requestDto.getTableName();
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		try{
			String sqlString ="SELECT * FROM " + TABLE_NAME + " WHERE ";
			Enumeration whereFields = wheres.keys(); 
			int idx=0;
			while(whereFields.hasMoreElements()){
				if (idx > 0){
					sqlString += "AND ";
				} else {
					sqlString += " ";
				}
				idx++;
				String where = (whereFields.nextElement()).toString();
				if ( null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
					sqlString += " " + where + " is null ";
				} else {
					sqlString += " " + where;
					sqlString += " = ? ";
				}
			}
	
			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			int paramIdx = 1;
			String whereValue;
			Enumeration whereFields_2ndRound = wheres.keys();
			while(whereFields_2ndRound.hasMoreElements()){
				try{
					String where = whereFields_2ndRound.nextElement().toString();
					if (null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
						log.debug("skipping null value for field: [" + where +"]");
					} else {
						whereValue = wheres.get(where).toString();
						pstmt.setString(paramIdx, whereValue);
						paramIdx++;
					}
				}catch (Exception e) {
					log.error("Could not set where condition");
				}
			}
			rs = pstmt.executeQuery();
		
			while(rs.next()){
		 		log.debug("rs.next()");
		 		responseDto  = new MetaDto();
		 		Set<String> keys;
		 		if (definedAttributes){
		 			// requestDto contiene i nomi delle colonne da stampare
		 			 keys = requestDto.keySet();
		 		} else {
		 			ResultSetMetaData md = rs.getMetaData() ;
		 			keys = new TreeSet<String>(); 
					for( int i = 1; i <= md.getColumnCount(); i++ ){
						keys.add(md.getColumnLabel(i));
						log.debug("FIELD_NAME:[" +md.getColumnLabel(i) +"]");
					}
		 		}
				Iterator<String> keysIterator2 = keys.iterator();
				String key;
				String value;
				while(keysIterator2.hasNext()){
					 key = keysIterator2.next() ;
					 value = rs.getString(key);
					 responseDto.put(key, rs.getString(key));
				     //pstmt.setString(idx,   value);
					 log.debug("Putting key: [" + key +"]  with value: + [" + value + "]"); 
					  idx++;	
				}
				listOfDto.add(responseDto);
		 	}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return listOfDto;
	}
	
	
	
	public String selectIdWhere(MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes, Integer tadRows ){	
		this.TABLE_NAME = requestDto.getTableName();
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		String id = null;
		try{
			//conn  = dbConnectionmanager.getConnection();
			String sqlString ="SELECT id FROM " + TABLE_NAME + " WHERE ";
			Enumeration whereFields = wheres.keys(); 
			int idx=0;
			while(whereFields.hasMoreElements()){
				if (idx > 0){
					sqlString += "AND ";
				} else {
					sqlString += " ";
				}
				idx++;
				String where = (whereFields.nextElement()).toString();
				if ( null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
					sqlString += " " + where + " is null ";
				} else {
					sqlString += " " + where;
					sqlString += " = ? ";
				}
			}
			if ( null != tadRows){
					sqlString += " limit " +tadRows.toString();
			}
			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			int paramIdx = 1;
			String whereValue;
			Enumeration whereFields_2ndRound = wheres.keys();
			while(whereFields_2ndRound.hasMoreElements()){
				try{
					String where = whereFields_2ndRound.nextElement().toString();
					if (null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
						log.debug("skipping null value for field: [" + where +"]");
					} else {
						whereValue = wheres.get(where).toString();
						pstmt.setString(paramIdx, whereValue);
						paramIdx++;
					}
				}catch (Exception e) {
					log.error("Could not set where condition");
				}
			}
			rs = pstmt.executeQuery();
		
			while(rs.next()){
				 id = rs.getString("id");
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return id;
	}
	
	public String selectCount(MetaDto requestDto, Hashtable wheres, Connection conn, boolean definedAttributes ){	
		this.TABLE_NAME = requestDto.getTableName();
		List listOfDto = new ArrayList();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		String CONTEGGIO ="";
		try{
			//conn  = dbConnectionmanager.getConnection();
			String sqlString ="SELECT count(*) as CONTEGGIO FROM " + TABLE_NAME + " WHERE ";
			Enumeration whereFields = wheres.keys(); 
			int idx=0;
			while(whereFields.hasMoreElements()){
				if (idx > 0){
					sqlString += "AND ";
				} else {
					sqlString += " ";
				}
				idx++;
				String where = (whereFields.nextElement()).toString();
				if ( null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
					sqlString += " " + where + " is null ";
				} else {
					sqlString += " " + where;
					sqlString += " = ? ";
				}
			}
			log.debug("sqlString = [" + sqlString + "]");
			pstmt = conn.prepareStatement(sqlString);
			int paramIdx = 1;
			String whereValue;
			Enumeration whereFields_2ndRound = wheres.keys();
			while(whereFields_2ndRound.hasMoreElements()){
				try{
					String where = whereFields_2ndRound.nextElement().toString();
					if (null == wheres.get(where) || (wheres.get(where).toString().equals(""))){
						log.debug("skipping null value for field: [" + where +"]");
					} else {
						whereValue = wheres.get(where).toString();
						pstmt.setString(paramIdx, whereValue);
						paramIdx++;
					}
				}catch (Exception e) {
					log.error("Could not set where condition");
				}
			}
			
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				CONTEGGIO = rs.getString("CONTEGGIO");
				
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return CONTEGGIO;
	}
	public boolean createTable(String tableName,List<String> fields, Connection conn){
		boolean isCreated = false;
		String sql ="CREATE TABLE "+ tableName +" (";
		String firstField;
		String nthField;
		Iterator<String> fieldIterator = fields.iterator();
		if (fields.size() == 0){
			return false;
		}else {
			firstField = fieldIterator.next();
			sql+=" "+firstField +" text";
		}
		while (fieldIterator.hasNext()){
			nthField = fieldIterator.next();
			sql+=", " +nthField +" text";
		}
		sql+=" );";
		try{
			PreparedStatement pstmt = null;
			log.debug("SQL:["+sql+"]");
			pstmt = conn.prepareStatement(sql);
			isCreated = pstmt.execute();
		}catch (Exception e) {
			log.error(e);
		}
		return isCreated;
	}
	
	
	public boolean dropTable(String tableName, Connection conn){
		boolean isDropped = false;
		try{
			String sqlString ="DROP TABLE "+ tableName +" ";
			PreparedStatement pstmt = null;
			pstmt = conn.prepareStatement(sqlString);
			isDropped = pstmt.execute();
		}catch (Exception e) {
			log.error(e);
		}
		return isDropped;
	}
	
	public List<MetaDto> runQuery(String sqlInputString, Connection conn ){	
		
		List<MetaDto> listOfDto = new ArrayList<MetaDto>();
		PreparedStatement pstmt = null;
		MetaDto responseDto = null;
		ResultSet rs;
		try{
			//int idx=0;
			log.debug("sqlInputString = [" + sqlInputString + "]");
			pstmt = conn.prepareStatement(sqlInputString);
			int paramIdx = 1;
			String whereValue;
			
			rs = pstmt.executeQuery();
		
			while(rs.next()){
		 		log.debug("rs.next()");
		 		responseDto  = new MetaDto();
		 		Set<String> keys;
		 	
		 		ResultSetMetaData md = rs.getMetaData() ;
		 		keys = new TreeSet<String>(); 
				for( int i = 1; i <= md.getColumnCount(); i++ ){
					keys.add(md.getColumnLabel(i));
						//log.debug("FIELD_NAME:[" +md.getColumnLabel(i) +"]");
				}
		 		

				Iterator<String> keysIterator2 = keys.iterator();
				String key;
				String value;
				while(keysIterator2.hasNext()){
					 key = keysIterator2.next() ;
					 value = rs.getString(key);
					 responseDto.put(key, rs.getString(key));
				     //pstmt.setString(idx,   value);
					 log.debug( "["+key +"]=[" + value + "]"); 
					 //idx++;	
				}
				listOfDto.add(responseDto);
		 	}
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try{
				pstmt.close();
			}catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return listOfDto;
	}
}
