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

import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.ddl.ColumnDefinition;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

//TODO javadoc
//TODO improve URL building to reduce duplication/boilerplate code

public class StorageAccess 
{
	private final String numRowsVar = "$nextn";
	private final Map<String, Keyspace> keyspaces = new ConcurrentHashMap<String, Keyspace>();
	String clusterName = System.getProperty("helena.cluster.name","cluster");
	String hostPortPairs = System.getProperty("helena.cluster.hosts", "localhost:9160");
	//TODO make configurable
	int defaultNumCols = 100;
	int defaultNumRows = 1; //TODO change this back!
	Cluster cluster;
	private String keyPrefix;
	
	private static final StringSerializer s = StringSerializer.get();
	
	public StorageAccess(String keyPrefix)
	{
		this.keyPrefix = "/" + keyPrefix;
		cluster = HFactory.getOrCreateCluster(clusterName, new CassandraHostConfigurator(hostPortPairs));
	}
	
	public String getNumRowsVar() {
		return numRowsVar;
	}

	public Keyspace getKeyspaceForName(String keyspace)
	{
		Keyspace rval = keyspaces.get(keyspace);
		if (rval == null)
		{
			rval = HFactory.createKeyspace(keyspace, cluster);
			keyspaces.put(keyspace, rval);
		}
		return rval;
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

			String comment = cf.getComment();
			Map<String, String> commentMap = new HashMap<String, String>();
			commentMap.put("comment", comment);
			rval.put(cfurl, commentMap);
		}
		return rval;
	}

	public Map<String, Map<String, String>> getColumnFamily(String keyspace, String cf) throws CharacterCodingException 
	{
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace.toString());
		List<ColumnFamilyDefinition> cfDef = keyspaceDef.getCfDefs();
		Map<String, Map<String, String>> rval = new HashMap<String, Map<String, String>>();
		for (ColumnFamilyDefinition cfentry : cfDef)
		{
			if (cfentry.getName().equals(cf))
			{
				//Handle Metadata
				Charset charset = Charset.forName("UTF8"); //TODO add handling for other types
				CharsetDecoder decoder = charset.newDecoder();

				Map<String, String> cfmeta = new HashMap<String, String>();
				for (ColumnDefinition colDef : cfentry.getColumnMetadata())
				{
					String colName = decoder.decode(colDef.getName()).toString();
					cfmeta.put("column_name", colName);
					cfmeta.put("validation_class", colDef.getValidationClass());
					cfmeta.put("index_name", colDef.getIndexName());
					cfmeta.put("index_type", colDef.getIndexType().toString());
				}
				rval.put("metadata", cfmeta);
				//Handle page link
				Map<String, String> link = new HashMap<String, String>();
				link.put("href", keyPrefix + '/' + keyspace + '/' + cf + "/?" + numRowsVar + "=" + defaultNumRows);
				rval.put("browse", link);
				//Handle search template
				//TODO check for existence of indexed column, use one as example col.
				Map<String, String> search = new HashMap<String, String>();
				search.put("href",  keyPrefix + '/' + keyspace + '/' + cf + "?email=somebody@example.com");
				//TODO I mean it, make the above link better.
				rval.put("search", search);
				break;
			}
		}
		return rval;
	}
	
	public Map<String, Map<String, String>> getRows(String keyspace, String cf, String startKey, String colRange, int numRows, boolean reverse, int numCols)
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		RangeSlicesQuery<String, String, String> rowsQuery = HFactory.createRangeSlicesQuery(ks, s, s, s);
		rowsQuery.setColumnFamily(cf);
		rowsQuery.setKeys(startKey, "");
		if (colRange != null)
		{
			String[] cols = colRange.split(",");
			rowsQuery.setRange(cols[0], cols[1], reverse, numCols);
		}
		else
		{
			rowsQuery.setRange("", "", reverse, numCols);
		}
		
		rowsQuery.setRowCount(numRows + 1);

		OrderedRows<String, String, String> result = rowsQuery.execute().get();
		return resultToMap(keyspace, cf, null, numRows, colRange, result);
	}

	public Map<String, Map<String, String>> getRows(String keyspace, String cf, String startKey, String colRange, int numRows)
	{
		return getRows(keyspace, cf, startKey, colRange, numRows, false, defaultNumCols);
	}
	
	public Map<String, Map<String, String>> getQueriedRows(String keyspace, String cf, String startKey, String colRange, String query, int numRows, boolean reverse, int numCols)
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		//TODO This code is really similar to code in getRows...
		//tried to do an extract-method here to configure the query passing the Query as a parameter,
		//however, RangeSlicesQuery and IndexSlicesQuery don't share a parent.
		//TODO Look at refactoring Hector to simplify this code.
		//TODO BUG:query hangs if query column is not in slice range.
		IndexedSlicesQuery<String, String, String> indexedSlicesQuery =
		HFactory.createIndexedSlicesQuery(ks, s, s, s);
		indexedSlicesQuery.setColumnFamily(cf);
		if (colRange != null)
		{
			String[] cols = colRange.split(",");
			indexedSlicesQuery.setRange(cols[0], cols[1], reverse, numCols);
		}
		else
		{
			indexedSlicesQuery.setRange("", "", reverse, numCols);
		}
		indexedSlicesQuery.setStartKey(startKey);
		indexedSlicesQuery.setRowCount(numRows + 1);

		//TODO ensure correct support for $vars in query string. (ie: don't add as hector query expressions)
		for (String clause : query.split("&"))
		{
			String[] keyval = clause.split("=");
			indexedSlicesQuery.addEqualsExpression(keyval[0], keyval[1]);
		}
		OrderedRows<String, String, String> result = indexedSlicesQuery.execute().get();
		return resultToMap(keyspace, cf, query, numRows, colRange, result);
	}
	
	public Map<String, Map<String, String>> getQueriedRows(String keyspace, String cf, String startKey, int numRows, String colRange, String columnQuery)
	{
		return getQueriedRows(keyspace, cf, startKey, colRange, columnQuery, numRows, false, defaultNumCols);
	}

	private Map<String, Map<String, String>> resultToMap(String keyspace,
			String cf, String query, int numRows, String colRange, OrderedRows<String, String, String> result) {
		Map<String, Map<String, String>> rval = new HashMap<String, Map<String, String>>();
		String keyurl = keyPrefix + '/' + keyspace + '/' + cf +'/';
		String colQueryPart = (query != null) ? query + "&": "";
		String matrix = (colRange != null) ? ";" + colRange: "";
		int processedRows = 0;
		for (Row<String,String,String> r : result)
		{
			String rowKey;
			//handle link to next page
			if (processedRows >= numRows)
			{
				rowKey = keyPrefix + '/' + keyspace + '/' + cf + "/" + r.getKey() + matrix + "?" + colQueryPart + numRowsVar + "=" + numRows;
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

	public void setColumn(String keyspace, String cf, String key, String col, String colVal) 
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		Mutator<String> mutator = HFactory.createMutator(ks, s);
		mutator.insert(key, cf, HFactory.createStringColumn(col, colVal));
	}

	public void removeColumn(String keyspace, String cf, String key, String column) 
	{
		Keyspace ks = getKeyspaceForName(keyspace);
		Mutator<String> mutator = HFactory.createMutator(ks, s);
		mutator.delete(key, cf, column, s);
	}

}
