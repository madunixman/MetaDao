package net.lulli.metadao;


import net.lulli.metadao.api.MetaDao;
import net.lulli.metadao.api.MetaDto;
import net.lulli.metadao.api.WheresMap;
import net.lulli.metadao.impl.MetaDtoImpl;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;


public class MetaDaoImpl implements MetaDao<Connection>
{
    static Logger log = Logger.getLogger("MetaDaoImpl");
    private String tableName;
    private String idField;

    protected MetaDaoImpl()
    {
        //
    }

    public String getIdField()
    {
        return this.idField;
    }

    protected MetaDaoImpl(String tableName)
    {
        this.tableName = tableName;
    }

    public boolean dropTable(String tableName, Connection dataConnection)
    {
        Connection conn = (Connection) dataConnection;
        boolean isDropped = false;
        String sqlString = "DROP TABLE ? ";
        try (PreparedStatement pstmt = conn.prepareStatement(sqlString))
        {
            pstmt.setString(1, tableName);
            isDropped = pstmt.execute();
        } catch (Exception e)
        {
            log.error(e);
        }
        return isDropped;
    }


    public Integer insert(MetaDto dto, Connection dataConnection)
    {
        Connection conn = (Connection) dataConnection;
        String retValue = insertLegacy(dto, conn);
        return Integer.valueOf(retValue);
    }

    private String insertLegacy(MetaDto dto, Connection conn)
    {

        this.tableName = dto.getTableName();
        log.debug("BEGIN insert()");
        String uuid = null;

        removeNullKey(dto);

        Set<String> keys = dto.keySet();
        Iterator<String> keysIterator = keys.iterator();
        Iterator<String> keysIteratorSymbols = keys.iterator();
        PreparedStatement pstmt = null;
        String k;
        boolean isFirstField = true;
        String sql = "INSERT INTO " + tableName + " " +
                "(";
        while (keysIterator.hasNext())
        {
            k = keysIterator.next();
            if (isFirstField)
            {
                sql = sql + " " + k;
                isFirstField = false;
            } else
            {
                sql = sql + ", " + k;
            }
        }

        sql = sql + ") values ( ";

        boolean isFirstPlaceHolder = true;
        while (keysIteratorSymbols.hasNext())
        {
            k = keysIteratorSymbols.next();
            if (isFirstPlaceHolder)
            {
                sql = sql + "? ";
                isFirstPlaceHolder = false;
            } else
            {
                sql = sql + ",? ";
            }
        }
        sql = sql +
                ")";
        log.debug("SQL=[" + sql + "]");
        try
        {
            log.debug("BEFORE prepareStatement()");
            pstmt = conn.prepareStatement(sql);
            log.debug("AFTER prepareStatement()");
            int idx = 1;
            Iterator<String> keysIterator2 = keys.iterator();
            String key;
            String value = "";
            while (keysIterator2.hasNext())
            {
                key = keysIterator2.next();
                Object kvalue = dto.get(key);
                if (null != kvalue)
                {
                    value = kvalue.toString();
                }
                pstmt.setString(idx, value);
                //IF cassandra uuid: pstmt.setObject(value.toCharArray(), value)
                log.trace("Adding key: [" + key + "] with index: [" + idx + "] with value: + [" + value + "]");
                idx++;
            }
            log.debug("BEFORE execute()");
            pstmt.execute();
            log.debug("AFTER execute()");
        } catch (Exception e)
        {
            log.error("Eccezione:" + e);
        } finally
        {
            try
            {
                if (null != pstmt)
                {
                    pstmt.close();
                }
            } catch (Exception e)
            {
                log.error(e);
            }
        }
        return uuid;
    }

    private void removeNullKey(MetaDto dto)
    {
        if (dto.containsKey(null))
        {
            dto.remove(null);
            log.debug("METADAO_INSERT: REMOVE_NULL");
        }
    }

    //TODO add return changed records
    public Integer update(MetaDto dto, WheresMap wheres, Connection dataConnection)
    {
        updateLegacy(dto, wheres, dataConnection);
        return new Integer(0);
    }

    private void updateLegacy(MetaDto dto, WheresMap wheres, Connection gconn)
    {
        Connection conn = (java.sql.Connection) gconn;
        this.tableName = dto.getTableName();
        log.debug("BEGIN insert()");
        MetaDtoImpl requestDto = (MetaDtoImpl) dto;
        log.debug("BEGIN insert()");
        //Connection conn = null;
        PreparedStatement pstmt = null;
        Set<String> keys = dto.keySet();
        Iterator<String> keysIterator_set = keys.iterator();

        boolean isFirst = true;
        String sql = "UPDATE " + tableName + " set ";
        int index = 0;
        String k;
        while (keysIterator_set.hasNext())
        {
            k = keysIterator_set.next();
            index++;
        }
        index = 1;
        Set<String> whereKeysP1 = wheres.keySet();
        Iterator<String> wheresIteratorP1 = whereKeysP1.iterator();
        String whereKey;
        String whereValue;
        boolean whereIteratorIsFirstP1 = true;
        while (wheresIteratorP1.hasNext())
        {
            whereKey = wheresIteratorP1.next();
            whereValue = wheres.get(whereKey).toString();
            if (whereIteratorIsFirstP1)
            {
                sql = sql + " where " + whereKey + " = ? ";
                whereIteratorIsFirstP1 = false;
            } else
            {
                sql = sql + "  AND " + whereKey + " = ? ";
            }
        }
        log.debug("SQL=[" + sql + "]");
        try
        {
            log.debug("BEFORE prepareStatement()");
            pstmt = conn.prepareStatement(sql);
            Iterator<String> keysIterator_parameter = keys.iterator();
            while (keysIterator_parameter.hasNext())
            {
                k = keysIterator_parameter.next();
                pstmt.setString(index, (String) requestDto.get(k));
                index++;
            }
            Set<String> whereKeysP2 = wheres.keySet();
            Iterator<String> wheresIteratorP2 = whereKeysP2.iterator();

            while (wheresIteratorP2.hasNext())
            {
                whereKey = wheresIteratorP2.next();
                whereValue = wheres.get(whereKey).toString();
                pstmt.setString(index, (String) wheres.get(whereKey));
                index++;
            }

            log.debug("AFTER prepareStatement()");
            log.debug("BEFORE execute()");
            pstmt.execute();
            log.debug("AFTER execute()");
        } catch (Exception e)
        {
            log.error("" + e);
        } finally
        {
            try
            {
                if (null != pstmt)
                {
                    pstmt.close();
                }
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
    }


    //TODO return number of deleted records
    public Integer delete(MetaDto dto, WheresMap wheres, Connection conn)
    {
        deleteLegacy(dto, wheres, conn);
        return new Integer(0);
    }

    @Deprecated
    void deleteLegacy(MetaDto dto, WheresMap wheres, Connection dataConnection)
    {
        Connection conn = (Connection) dataConnection;
        this.tableName = dto.getTableName();
        log.debug("BEGIN insert()");
        MetaDtoImpl requestDto = (MetaDtoImpl) dto;
        log.debug("BEGIN insert()");
        PreparedStatement pstmt = null;
        Set<String> keys = dto.keySet();
        Iterator<String> keysIterator_set = keys.iterator();

        String sql = "DELETE FROM " + tableName + " ";
        int index = 0;
        String k;
        index = 1;
        Set<String> whereKeysP1 = wheres.keySet();
        Iterator<String> wheresIteratorP1 = whereKeysP1.iterator();
        String whereKey;
        String whereValue;
        boolean whereIteratorIsFirstP1 = true;
        while (wheresIteratorP1.hasNext())
        {
            whereKey = wheresIteratorP1.next();
            whereValue = wheres.get(whereKey).toString();
            if (whereIteratorIsFirstP1)
            {
                sql = sql + " where " + whereKey + " = ? ";
                whereIteratorIsFirstP1 = false;
            } else
            {
                sql = sql + "  AND " + whereKey + " = ? ";
            }
        }
        log.debug("SQL=[" + sql + "]");
        try
        {
            log.debug("BEFORE prepareStatement()");
            pstmt = conn.prepareStatement(sql);
            Iterator<String> keysIterator_parameter = keys.iterator();
            while (keysIterator_parameter.hasNext())
            {
                k = keysIterator_parameter.next();
                pstmt.setString(index, (String) requestDto.get(k));
                index++;
            }
            Set<String> whereKeysP2 = wheres.keySet();
            Iterator<String> wheresIteratorP2 = whereKeysP2.iterator();

            while (wheresIteratorP2.hasNext())
            {
                whereKey = wheresIteratorP2.next();
                whereValue = wheres.get(whereKey).toString();
                pstmt.setString(index, (String) wheres.get(whereKey));
                index++;
            }
            log.debug("AFTER prepareStatement()");
            log.debug("BEFORE execute()");
            pstmt.execute();
            log.debug("AFTER execute()");
        } catch (Exception e)
        {
            log.error("" + e);
        } finally
        {
            try
            {
                if (null != pstmt)
                {
                    pstmt.close();
                }
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
    }

    public MetaDtoImpl descTable(String tableName, Connection dataConnection)
    {
        Connection conn = (Connection) dataConnection;
        this.tableName = tableName;
        List listOfDto = new ArrayList();
        MetaDtoImpl responseDto = null;
        String sqlString = "select * from ? limit 1";

        int idx = 0;
        log.debug("sqlString = [" + sqlString + "]");
        try (PreparedStatement pstmt = conn.prepareStatement(sqlString))
        {
            pstmt.setString(1, this.tableName);

            try (ResultSet rs = pstmt.executeQuery())
            {
                while (rs.next())
                {
                    log.debug("rs.next()");
                    responseDto = new MetaDtoImpl();
                    Set<String> keys;

                    ResultSetMetaData md = rs.getMetaData();
                    keys = new TreeSet<String>();
                    for (int i = 1; i <= md.getColumnCount(); i++)
                    {
                        keys.add(md.getColumnLabel(i));
                    }
                    Iterator<String> keysIterator2 = keys.iterator();
                    String key;
                    String value;
                    while (keysIterator2.hasNext())
                    {
                        key = keysIterator2.next();
                        value = rs.getString(key);
                        responseDto.put(key, rs.getString(key));
                        log.debug("[" + key + "]=[" + value + "]");
                        idx++;
                    }
                }
                if (null != rs)
                {
                    rs.close();
                }
            }

            if (null != pstmt)
            {
                pstmt.close();
            }
        } catch (Exception e)
        {
            log.error(e.getMessage());
        }
        return responseDto;
    }


    public List select(MetaDtoImpl requestDto, WheresMapImpl wheres, boolean definedAttributes, Connection dataConnection)
    {
        Connection conn = (java.sql.Connection) dataConnection;
        this.tableName = requestDto.getTableName();
        List listOfDto = new ArrayList();
        PreparedStatement pstmt = null;
        MetaDtoImpl responseDto = null;
        ResultSet rs = null;
        try
        {
            String sqlString = "SELECT * FROM " + tableName + " WHERE ";
            Enumeration whereFields = wheres.keys();
            int idx = 0;
            while (whereFields.hasMoreElements())
            {
                if (idx > 0)
                {
                    sqlString += "AND ";
                } else
                {
                    sqlString += " ";
                }
                idx++;
                String where = (whereFields.nextElement()).toString();
                if (null == wheres.get(where) || (wheres.get(where).toString().equals("")))
                {
                    sqlString += " " + where + " is null ";
                } else
                {
                    sqlString += " " + where;
                    sqlString += " = ? ";
                }
            }

            log.debug("sqlString = [" + sqlString + "]");
            pstmt = conn.prepareStatement(sqlString);
            int paramIdx = 1;
            String whereValue;
            Enumeration whereFields_2ndRound = wheres.keys();
            while (whereFields_2ndRound.hasMoreElements())
            {
                try
                {
                    String where = whereFields_2ndRound.nextElement().toString();
                    if (null == wheres.get(where) || (wheres.get(where).toString().equals("")))
                    {
                        log.debug("skipping null value for field: [" + where + "]");
                    } else
                    {
                        whereValue = wheres.get(where).toString();
                        pstmt.setString(paramIdx, whereValue);
                        paramIdx++;
                    }
                } catch (Exception e)
                {
                    log.error("Could not set where condition");
                }
            }

            rs = pstmt.executeQuery();

            while (rs.next())
            {
                log.debug("rs.next()");
                responseDto = new MetaDtoImpl();
                Set<String> keys;
                if (definedAttributes)
                {
                    // requestDto contiene i nomi delle colonne da stampare
                    keys = requestDto.keySet();
                } else
                {
                    ResultSetMetaData md = rs.getMetaData();
                    keys = new TreeSet<String>();
                    for (int i = 1; i <= md.getColumnCount(); i++)
                    {
                        keys.add(md.getColumnLabel(i));
                        log.debug("FIELD_NAME:[" + md.getColumnLabel(i) + "]");
                    }
                }
                Iterator<String> keysIterator2 = keys.iterator();
                String key;
                String value;
                while (keysIterator2.hasNext())
                {
                    key = keysIterator2.next();
                    value = rs.getString(key);
                    responseDto.put(key, rs.getString(key));
                    log.debug("Putting key: [" + key + "]  with value: + [" + value + "]");
                    idx++;
                }
                listOfDto.add(responseDto);
            }
        } catch (Exception e)
        {
            log.error(e.getMessage());
        } finally
        {
            if (null != rs)
            {
                try
                {
                    rs.close();
                } catch (SQLException e)
                {
                    log.error(e.getMessage());
                }
            }
            try
            {
                if (null != pstmt)
                pstmt.close();
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
        return listOfDto;
    }

    public String selectIdWhere(MetaDto requestDto, WheresMapImpl wheres, Connection dataConnection, boolean definedAttributes, Integer tadRows)
    {
        Connection conn = (java.sql.Connection) dataConnection;
        this.tableName = requestDto.getTableName();
        List listOfDto = new ArrayList();
        PreparedStatement pstmt = null;
        MetaDtoImpl responseDto = null;
        ResultSet rs = null;
        String id = null;
        try
        {
            String sqlString = "SELECT id FROM " + tableName + " WHERE ";
            Enumeration whereFields = wheres.keys();
            int idx = 0;
            while (whereFields.hasMoreElements())
            {
                if (idx > 0)
                {
                    sqlString += "AND ";
                } else
                {
                    sqlString += " ";
                }
                idx++;
                String where = (whereFields.nextElement()).toString();
                if (null == wheres.get(where) || (wheres.get(where).toString().equals("")))
                {
                    sqlString += " " + where + " is null ";
                } else
                {
                    sqlString += " " + where;
                    sqlString += " = ? ";
                }
            }
            if (null != tadRows)
            {
                sqlString += " limit " + tadRows.toString();
            }
            log.debug("sqlString = [" + sqlString + "]");
            pstmt = conn.prepareStatement(sqlString);
            int paramIdx = 1;
            String whereValue;
            Enumeration whereFields_2ndRound = wheres.keys();
            while (whereFields_2ndRound.hasMoreElements())
            {
                try
                {
                    String where = whereFields_2ndRound.nextElement().toString();
                    if (null == wheres.get(where) || (wheres.get(where).toString().equals("")))
                    {
                        log.debug("skipping null value for field: [" + where + "]");
                    } else
                    {
                        whereValue = wheres.get(where).toString();
                        pstmt.setString(paramIdx, whereValue);
                        paramIdx++;
                    }
                } catch (Exception e)
                {
                    log.error("Could not set where condition");
                }
            }
            rs = pstmt.executeQuery();

            while (rs.next())
            {
                id = rs.getString("id");
            }
        } catch (Exception e)
        {
            log.error(e.getMessage());
        } finally
        {
            try
            {
                if (null != rs)
                    rs.close();
            } catch (SQLException e)
            {
                log.error(e.getMessage());
            }
            try
            {
                if (null != pstmt)
                pstmt.close();
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
        return id;
    }


    public Integer selectCount(net.lulli.metadao.api.MetaDto requestDto, WheresMapImpl wheres,
                               Connection dataConnection, boolean definedAttributes)
    {
        Connection conn = (java.sql.Connection) dataConnection;
        String retValue = selectCountLegacy(requestDto, wheres, conn, definedAttributes);
        return Integer.valueOf(retValue);
    }

    private String selectCountLegacy(net.lulli.metadao.api.MetaDto requestDto, WheresMapImpl wheres, Connection conn, boolean definedAttributes)
    {
        this.tableName = requestDto.getTableName();
        List listOfDto = new ArrayList();
        PreparedStatement pstmt = null;
        MetaDtoImpl responseDto = null;
        ResultSet rs = null;
        String CONTEGGIO = "";
        try
        {
            String sqlString = "SELECT count(*) as CONTEGGIO FROM " + tableName + " WHERE ";
            Enumeration whereFields = wheres.keys();
            int idx = 0;
            while (whereFields.hasMoreElements())
            {
                if (idx > 0)
                {
                    sqlString += "AND ";
                } else
                {
                    sqlString += " ";
                }
                idx++;
                String where = (whereFields.nextElement()).toString();
                if (null == wheres.get(where) || (wheres.get(where).toString().equals("")))
                {
                    sqlString += " " + where + " is null ";
                } else
                {
                    sqlString += " " + where;
                    sqlString += " = ? ";
                }
            }
            log.debug("sqlString = [" + sqlString + "]");
            pstmt = conn.prepareStatement(sqlString);
            int paramIdx = 1;
            String whereValue;
            Enumeration whereFields_2ndRound = wheres.keys();
            while (whereFields_2ndRound.hasMoreElements())
            {
                try
                {
                    String where = whereFields_2ndRound.nextElement().toString();
                    if (null == wheres.get(where) || (wheres.get(where).toString().equals("")))
                    {
                        log.debug("skipping null value for field: [" + where + "]");
                    } else
                    {
                        whereValue = wheres.get(where).toString();
                        pstmt.setString(paramIdx, whereValue);
                        paramIdx++;
                    }
                } catch (Exception e)
                {
                    log.error("Could not set where condition");
                }
            }
            rs = pstmt.executeQuery();
            while (rs.next())
            {
                CONTEGGIO = rs.getString("CONTEGGIO");
            }
        } catch (Exception e)
        {
            log.error(e.getMessage());
        } finally
        {
            try
            {
                if (null != rs)
                    rs.close();
            } catch (SQLException e)
            {
                log.error(e.getMessage());
            }
            try
            {
                if (null != pstmt)
                pstmt.close();
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
        return CONTEGGIO;
    }

    //SONAR BLOCK: string concatenation
    @Deprecated
    public boolean createTable(String tableName, List<String> fields, Connection dataConnection)
    {
        /*
        Connection conn = (Connection) dataConnection;
        boolean isCreated = false;
        String sql = "CREATE TABLE " + tableName + " (";
        String firstField;
        String nthField;
        Iterator<String> fieldIterator = fields.iterator();
        if (fields.size() == 0)
        {
            return false;
        } else
        {
            firstField = fieldIterator.next();
            sql += " " + firstField + " text";
        }
        while (fieldIterator.hasNext())
        {
            nthField = fieldIterator.next();
            sql += ", " + nthField + " text";
        }
        sql += " );";
        try
        {
            log.debug("SQL:[" + sql + "]");
            try (PreparedStatement pstmt = conn.prepareStatement(sql))
            {
                isCreated = pstmt.execute();
            }
        } catch (Exception e)
        {
            log.error(e);
        }
        return isCreated;
        */
        return false;
    }


    public List<MetaDto> runQuery(String sqlInputString, Connection dataConnection)
    {
        Connection conn = (Connection) dataConnection;
        List<net.lulli.metadao.api.MetaDto> listOfDto = new ArrayList<net.lulli.metadao.api.MetaDto>();
        PreparedStatement pstmt = null;
        MetaDtoImpl responseDto = null;
        ResultSet rs = null;
        try
        {
            log.debug("sqlInputString = [" + sqlInputString + "]");
            pstmt = conn.prepareStatement(sqlInputString);
            int paramIdx = 1;
            String whereValue;

            rs = pstmt.executeQuery();

            while (rs.next())
            {
                log.debug("rs.next()");
                responseDto = new MetaDtoImpl();
                Set<String> keys;

                ResultSetMetaData md = rs.getMetaData();
                keys = new TreeSet<String>();
                for (int i = 1; i <= md.getColumnCount(); i++)
                {
                    keys.add(md.getColumnLabel(i));
                }

                Iterator<String> keysIterator2 = keys.iterator();
                String key;
                String value;
                while (keysIterator2.hasNext())
                {
                    key = keysIterator2.next();
                    value = rs.getString(key);
                    responseDto.put(key, rs.getString(key));
                    log.debug("[" + key + "]=[" + value + "]");
                }
                listOfDto.add(responseDto);
            }
        } catch (Exception e)
        {
            log.error(e.getMessage());
        } finally
        {
            try
            {
                rs.close();
            } catch (SQLException e)
            {
                log.error(e.getMessage());
            }
            try
            {
                pstmt.close();
            } catch (Exception e)
            {
                log.error(e.getMessage());
            }
        }
        return listOfDto;
    }

    @Deprecated
    public String getDialect()
    {
        return null;
    }

    @Deprecated
    public void setDialect(String s)
    {

    }


}