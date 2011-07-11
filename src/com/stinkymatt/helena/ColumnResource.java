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

import java.util.Map;

import org.restlet.resource.Delete;
import org.restlet.resource.Get;
import org.restlet.resource.Put;

public final class ColumnResource extends AbstractCassandraResource
{
	@Get
	public Map<String, String> getColumn() 
	{ 
		return parentApp.getStorage().getColumn(keyspace, cf, key, column);
	}
	
	@Put
	public void setColumn(String colVal)
	{
		parentApp.getStorage().setColumn(keyspace, cf, key, column, colVal);
	}
	
	@Delete
	public void removeColumn()
	{
		parentApp.getStorage().removeColumn(keyspace, cf, key, column);
	}
}
