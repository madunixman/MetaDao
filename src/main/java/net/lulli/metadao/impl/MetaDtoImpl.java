package net.lulli.metadao.impl;

import java.util.LinkedHashMap;

public class MetaDtoImpl extends LinkedHashMap implements net.lulli.metadao.api.MetaDto
{
    private String tableName;
    private String recordType;


    public static net.lulli.metadao.api.MetaDto of(String tableName)
    {
        net.lulli.metadao.api.MetaDto dto = new MetaDtoImpl();
        dto.setTableName(tableName);
        return dto;
    }

    public net.lulli.metadao.api.MetaDto getNewInstance(String tableName)
    {
        net.lulli.metadao.api.MetaDto dto = new MetaDtoImpl();
        dto.setTableName(tableName);
        return dto;
    }

    public void put(String key, String value)
    {
        super.put(key, value);
    }

    public String get(String key)
    {
        Object o = super.get(key);
        if (null != o)
        {
            return o.toString();
        }
        else
        {
            return null;
        }
    }

    public boolean containsKey(String key)
    {
        return super.containsKey(key);
    }

    public void remove(String key)
    {
        super.remove(key);
    }

    public String getTableName()
    {
        return this.tableName;
    }

    public void setTableName(String tableName)
    {
        this.tableName = tableName;
    }

    @Deprecated
    public String getRecordType()
    {
        return recordType;
    }

    @Deprecated
    public void setRecordType(String recordType)
    {
        this.recordType = recordType;
    }
}
