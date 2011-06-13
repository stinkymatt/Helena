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

import org.restlet.Component;
import org.restlet.Server;
import org.restlet.data.Protocol;
import org.restlet.Application;

public final class HelenaDaemon implements Runnable
{
	private Component serviceComponent;
	private int port = 8181;
	private String maxThreads = "128";
	private String context = "helena";
	private Application app = new HelenaApp(context);
	

	public HelenaDaemon()
	{
		serviceComponent = new Component();
		Server httpServer = new Server(Protocol.HTTP, port);
		serviceComponent.getServers().add(httpServer);
		httpServer.getContext().getParameters().add("maxThreads", maxThreads);
		serviceComponent.getDefaultHost().attach("/" + context, app);
	}

	@Override
	public void run()
	{
		try
		{
			serviceComponent.start();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args)
	{
		new Thread(new HelenaDaemon()).start();
	}
}
