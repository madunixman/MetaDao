package net.lulli.metadao.helper.generic;

import java.util.Hashtable;

public class TypeHelper {
	
	public static Hashtable<String, String> add(Hashtable h, String k, String v)
	{
		if (h == null)
		{
			h = new Hashtable<String, String>();
		}
		h.put(k, v);
		return h;
	}
}
