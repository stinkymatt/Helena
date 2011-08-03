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

import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.Application;
//import org.restlet.Context;
import org.restlet.data.Method;
import org.restlet.routing.Router;
import org.restlet.security.MethodAuthorizer;

public final class HelenaApp extends Application
{
	public static final String DEFAULT_ROWS = "1";
	private StorageAccess storage;
	private final String contextRoot;
	
	public HelenaApp(String tld)
	{
		super();
		this.contextRoot = tld;
	}
	
	public String getContextRoot() { return contextRoot; }

	//TODO Confirm this works in GAE
	public HelenaApp(Context ctx)
	{
		super(ctx);
		this.contextRoot = (String) ctx.getAttributes().get("org.restlet.ext.servlet.offsetPath");
	}

	public StorageAccess getStorage() 
	{
		return storage;
	}

	public synchronized Restlet createInboundRoot()
	{
		this.getTunnelService().setExtensionsTunnel(true);
		
		//TODO Add configurability for these lines
		storage = new StorageAccess(this.contextRoot);
		HelenaSimpleAuthenticator authenticator = new HelenaSimpleAuthenticator(getContext(), true);
		MethodAuthorizer authorizer = new MethodAuthorizer();
		authorizer.getAnonymousMethods().add(Method.GET);
		authorizer.getAuthenticatedMethods().add(Method.GET);
		authorizer.getAuthenticatedMethods().add(Method.PUT);
		authorizer.getAuthenticatedMethods().add(Method.DELETE);
		authenticator.setNext(authorizer);
		
		Router router = new Router(getContext());
		router.attach("", com.stinkymatt.helena.DBResource.class);
		router.attach("/{keyspace}", com.stinkymatt.helena.KSResource.class);
		router.attach("/{keyspace}/{cf}", com.stinkymatt.helena.CFResource.class);
		router.attach("/{keyspace}/{cf}/", com.stinkymatt.helena.KeyResource.class);//TODO treat this as FOUND? 
		router.attach("/{keyspace}/{cf}/{key}", com.stinkymatt.helena.KeyResource.class);
		router.attach("/{keyspace}/{cf}/{key}/{column}", com.stinkymatt.helena.ColumnResource.class);
		//return router;
		authorizer.setNext(router);
		return authenticator;
	}

}
