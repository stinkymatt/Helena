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

import org.restlet.resource.ServerResource;
//import org.restlet.resource.Get;

public abstract class AbstractCassandraResource extends ServerResource
{
	HelenaApp parentApp;
	String keyspace;
	String cf;
	String key;
	String lastkey;
	String column;

	public void configureResource()
	{
		keyspace = (String) getRequestAttributes().get("keyspace");
		cf = (String) getRequestAttributes().get("cf");
		key = (String) getRequestAttributes().get("key");
		//lastkey = (String) getRequestAttributes().get("lastkey");
		column = (String) getRequestAttributes().get("column");
	}

	public void doInit()
	{
		parentApp = (HelenaApp)getApplication();
		configureResource();
	}

	/* Handy for debugging...	
	@Get
	public String toString() 
	{ 
		return 
		"class: " + this.getClass().getName() + "\n" +
		"original ref:" + getRequest().getOriginalRef().getPath() + "\n" +
		"resource ref:" + getRequest().getResourceRef().getPath() + "\n" +
		"root ref:" + getRequest().getRootRef().getPath() + "\n";
	}
	*/
}
