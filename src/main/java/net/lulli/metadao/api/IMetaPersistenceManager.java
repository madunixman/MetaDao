package net.lulli.metadao.api;


import java.util.Hashtable;
import java.util.List;
import net.lulli.metadao.DbConnectionManager;
import net.lulli.metadao.model.SQLDialect;


public interface IMetaPersistenceManager extends ISQLDialect{
	
	String SQL_DIALECT = SQLDialect.STANDARD;
	public abstract DbConnectionManager getDbConnectionManager();
	
	public List search(MetaDto requestDto, Hashtable wheres);
	
	public List search(MetaDto requestDto, Hashtable wheres,  boolean definedAttributes, Integer resultRows);
	
	public  void insert(MetaDto dto);
	
	public void update(MetaDto dto, Hashtable wheres);
	
	public void delete(MetaDto dto, Hashtable wheres);
	
	public String save(MetaDto dto,  Hashtable wheres);
	
	public Integer selectCount(MetaDto requestDto, Hashtable wheres, boolean definedAttributes );
	
	public List<String> descTable(String tableName);
	
	public boolean createTable(String tableName,List<String> fields);
	
	public boolean dropTable(String tableName);
	
	public String getSQLDialect();
	
	public void setSQLDialect(String sqlDialect);
	
	public List<MetaDto> runQuery(String sqlInputString);
	
	public int execute(String sqlInputString );

}