/*
Copyright 2011 Matthew Kennedy

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package com.stinkymatt.helena;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

//TODO add search on column values
//TODO javadoc
//TODO efficiently reuse keyspace objects

public class StorageAccess 
{
	String clusterName = System.getProperty("helena.cluster.name","cluster");
	//TODO make configurable
	int defaultNumCols = 100;
	int defaultNumRows = 1; //TODO change this back!
	Cluster cluster;
	private String keyPrefix;
	
	private static StringSerializer s = StringSerializer.get();
	
	public StorageAccess(String keyPrefix)
	{
		this.keyPrefix = "/" + keyPrefix;
		//TODO add connection configuration
		cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator("localhost:9160"));
	}
	
	public Keyspace getKeyspaceForName(String keyspace)
	{
		//TODO Add hashmap for mutiple keyspace connections
		return HFactory.createKeyspace(keyspace, cluster);
	}
	
	public Map<String, Map<String,String>> getKeyspaces()
	{
		Map<String, Map<String,String>> rval = new HashMap<String, Map<String,String>>();
		for ( KeyspaceDefinition k : cluster.describeKeyspaces())
		{
			if (!k.getName().equals("system"))
			{
				rval.put(keyPrefix + '/' + k.getName(), k.getStrategyOptions());
			}
		}
		//TODO add empty check
		return rval;
	}
	
	public Map<String, Map<String, String>> getColumnFamilies(String keyspace)
	{
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace.toString());
		List<ColumnFamilyDefinition> cfDef = keyspaceDef.getCfDefs();
		Map<String, Map<String, String>> rval = new HashMap<String, Map<String, String>>();
		for (ColumnFamilyDefinition cf : cfDef)
		{
			String cfurl = keyPrefix + '/' + keyspace + '/' + cf.getName();
			Map<String, String> cfmeta = new HashMap<String, String>();
			for (ColumnDefinition colDef : cf.getColumnMetadata())
			{
				cfmeta.put("column_name", colDef.toString());
				cfmeta.put("validation_class", colDef.getValidationClass());
				cfmeta.put("index_name", colDef.getIndexName());
				cfmeta.put("index_type", colDef.getIndexType().toString());
			}
			if (cfmeta.isEmpty()) cfmeta.put("metadata", "n/a");
			rval.put(cfurl, cfmeta);
		}
		return rval;
	}
	

	public Map<String, String> getColumnsForKey(String keyspace, String cf, String key, boolean reverse, int numCols)
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		SliceQuery<String, String, String> q = HFactory.createSliceQuery(ks, s, s, s);
		q.setColumnFamily(cf);
		q.setKey(key);
		//TODO Add support for paging columns
		q.setRange("", "", reverse, numCols);
		QueryResult<ColumnSlice<String, String>> cols = q.execute();
		HashMap<String, String> rval = new HashMap<String, String>();
		String urlkey = keyPrefix + '/' + keyspace + '/' + cf +'/' + key + '/';
		for(HColumn<String, String> col : cols.get().getColumns())
		{
			rval.put(urlkey + col.getName(), col.getValue());
		}
		return rval;
	}

	public Map<String, String> getColumnsForKey(String keyspace, String cf, String key)
	{
		return getColumnsForKey(keyspace, cf, key, false, defaultNumCols);
	}
	
	public Map<String, Map<String, String>> getRows(String keyspace, String cf, String startKey, int numRows, boolean reverse, int numCols)
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		RangeSlicesQuery<String, String, String> rowsQuery = HFactory.createRangeSlicesQuery(ks, s, s, s);
		rowsQuery.setColumnFamily(cf);
		rowsQuery.setKeys(startKey, "");
		rowsQuery.setRange("", "", reverse, numCols);
		rowsQuery.setRowCount(numRows + 1);

		OrderedRows<String, String, String> result = rowsQuery.execute().get();
		Map<String, Map<String, String>> rval = new HashMap<String, Map<String, String>>();
		String keyurl = keyPrefix + '/' + keyspace + '/' + cf +'/';
		int processedRows = 0;
		for (Row<String,String,String> r : result)
		{
			String rowKey;
			//handle link to next page
			if (processedRows >= numRows)
			{
				rowKey = keyPrefix + '/' + keyspace + '/' + cf + "?startKey=" + r.getKey() + "&numRows=" + numRows;
				//assert r.getKey == result.peekLast().getKey()?
				Map<String, String> link = new HashMap<String, String>();
				link.put("href", rowKey);
				rval.put("next", link);
			}
			else
			{
				rowKey = keyurl + r.getKey();
				Map<String, String> columns = new HashMap<String,String>();
				for (HColumn<String, String>col: r.getColumnSlice().getColumns())
				{
					columns.put(col.getName(), col.getValue());
				}
				rval.put(rowKey, columns);
			}
			processedRows++;
		}
		return rval;
	}
	
	public Map<String, Map<String, String>> getRows(String keyspace, String cf, String startKey, int numRows)
	{
		return getRows(keyspace, cf, startKey, numRows, false, defaultNumCols);
	}
	
	public Map<String, String> getColumn(String keyspace, String cf, String key, String colName)
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(ks);
		columnQuery.setColumnFamily(cf).setKey(key).setName(colName);
		HColumn<String, String> result = columnQuery.execute().get();
		Map<String, String> rval = new HashMap<String, String>();
		String urlkey = keyPrefix + '/' + keyspace + '/' + cf +'/' + '/' + key + '/' + result.getName();
		rval.put(urlkey, result.getValue());
		return rval;
	}
}
