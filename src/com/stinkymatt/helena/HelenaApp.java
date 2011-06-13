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

import org.restlet.Restlet;
import org.restlet.Application;
//import org.restlet.Context;
import org.restlet.routing.Router;

public final class HelenaApp extends Application
{
	private final StorageAccess storage;
	private final String contextRoot;
	
	public HelenaApp(String tld)
	{
		this.contextRoot = tld;
		storage = new StorageAccess(tld);
		this.getTunnelService().setExtensionsTunnel(true);
	}
	
	public String getContextRoot() { return contextRoot; }

	//TODO Figure out when this should be used...
//	public HelenaApp(Context ctx)
//	{
//		storage = new StorageAccess();
//		this.getTunnelService().setExtensionsTunnel(true);
//	}

	public StorageAccess getStorage() {
		return storage;
	}

	public synchronized Restlet createInboundRoot()
	{
		Router router = new Router(getContext());
		router.attach("", DBResource.class);
		router.attach("/{keyspace}", KSResource.class);
		router.attach("/{keyspace}/{cf}", CFResource.class);
		router.attach("/{keyspace}/{cf}/{key}", KeyResource.class);
		router.attach("/{keyspace}/{cf}/{key}/{column}", ColumnResource.class);
		return router;
	}

}
