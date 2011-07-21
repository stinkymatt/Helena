package com.stinkymatt.helena;

import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.security.Authenticator;
import org.restlet.security.Enroler;

public class HelenaSimpleAuthenticator extends Authenticator 
{
	public static final boolean allowWrite = Boolean.parseBoolean(System.getProperty("helena.auth.simple", "false"));
	
	/**
     * Constructor setting the mode to "required".
     * 
     * @param context
     *            The context.
     * @see #Authenticator(Context)
     */
    public HelenaSimpleAuthenticator(Context context) {
        super(context);
    }

    /**
     * Constructor using the context's default enroler.
     * 
     * @param context
     *            The context.
     * @param optional
     *            The authentication mode.
     * @see #Authenticator(Context, boolean, Enroler)
     */
    public HelenaSimpleAuthenticator(Context context, boolean optional) {
        super(context, optional);
    }

    /**
     * Constructor.
     * 
     * @param context
     *            The context.
     * @param optional
     *            The authentication mode.
     * @param enroler
     *            The enroler to invoke upon successful authentication.
     */
    public HelenaSimpleAuthenticator(Context context, boolean optional, Enroler enroler) {
        super(context, optional, enroler);
    }


	@Override
	protected boolean authenticate(Request arg0, Response arg1) 
	{
		return allowWrite;
	}

}
