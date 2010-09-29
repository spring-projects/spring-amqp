/*
 * Copyright 2002-2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.erlang.connection;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.erlang.OtpIOException;
import org.springframework.util.Assert;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpPeer;
import com.ericsson.otp.erlang.OtpSelf;

/**
 * A simple implementation of {@link ConnectionFactory} that return a new connection 
 * for each invocation of the createConnection method.  
 * 
 * Note that use of this ConnectionFactory with ErlangTemplate has unstable behavior when
 * invoked frequently and will be deprecated.  See {@link SingleConnectionFactory} for an
 * alternative implementation.
 * 
 * 
 * Provides a more traditional API to creating a connection to a remote erlang node than
 * the JInterface API.
 * 
 * <p>The following is taken from the JInterface javadocs that describe the valid
 * node names that can be used.  These naming constraints apply to the string values
 * you pass into the node names in SimpleConnectionFactory's constructor. 
 * <p>
 * About nodenames: Erlang nodenames consist of two components, an alivename and
 * a hostname separated by '@'. Additionally, there are two nodename formats:
 * short and long. Short names are of the form "alive@hostname", while long
 * names are of the form "alive@host.fully.qualified.domainname". Erlang has
 * special requirements regarding the use of the short and long formats, in
 * particular they cannot be mixed freely in a network of communicating nodes,
 * however Jinterface makes no distinction. See the Erlang documentation for
 * more information about nodenames.
 * </p>
 * 
 * <p>
 * The constructors for the AbstractNode classes will create names exactly as
 * you provide them as long as the name contains '@'. If the string you provide
 * contains no '@', it will be treated as an alivename and the name of the local
 * host will be appended, resulting in a shortname. Nodenames longer than 255
 * characters will be truncated without warning.
 * </p>
 * 
 * <p>
 * Upon initialization, this class attempts to read the file .erlang.cookie in
 * the user's home directory, and uses the trimmed first line of the file as the
 * default cookie by those constructors lacking a cookie argument. If for any
 * reason the file cannot be found or read, the default cookie will be set to
 * the empty string (""). The location of a user's home directory is determined
 * using the system property "user.home", which may not be automatically set on
 * all platforms.
 * </p>
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class SimpleConnectionFactory implements ConnectionFactory, InitializingBean {

	protected final Log logger = LogFactory.getLog(getClass());
	
	private boolean uniqueSelfNodeName = true;
	
	private String selfNodeName;

	private String cookie;

	private String peerNodeName;

	private OtpSelf otpSelf;

	private OtpPeer otpPeer;


	public SimpleConnectionFactory(String selfNodeName, String cookie, String peerNodeName) {
		this(selfNodeName, peerNodeName);
		this.cookie = cookie;
	}

	public SimpleConnectionFactory(String selfNodeName, String peerNodeName) {
		this.selfNodeName = selfNodeName;
		this.peerNodeName = peerNodeName;
	}


	public Connection createConnection() throws UnknownHostException, OtpAuthException, IOException {
		try {
			return new DefaultConnection(otpSelf.connect(otpPeer));
		}
		catch (IOException ex) {
			throw new OtpIOException("failed to connect from '" + this.selfNodeName
					+ "' to peer node '" + this.peerNodeName + "'", ex);
		}
	}
	
	public boolean isUniqueSelfNodeName() {
		return uniqueSelfNodeName;
	}

	public void setUniqueSelfNodeName(boolean uniqueSelfNodeName) {
		this.uniqueSelfNodeName = uniqueSelfNodeName;
	}

	public void afterPropertiesSet() {
		Assert.isTrue(this.selfNodeName != null || this.peerNodeName != null,
				"'selfNodeName' or 'peerNodeName' is required");
		String selfNodeNameToUse = this.selfNodeName;
		if (isUniqueSelfNodeName()) {
			selfNodeNameToUse = this.selfNodeName + "-" + UUID.randomUUID().toString();
			logger.debug("Creating OtpSelf with node name = [" + selfNodeNameToUse + "]");
		}
		try {
			if (this.cookie == null) {
				this.otpSelf = new OtpSelf(selfNodeNameToUse.trim());
			}
			else {
				this.otpSelf = new OtpSelf(selfNodeNameToUse.trim(), this.cookie);
			}
		}
		catch (IOException e) {
			throw new OtpIOException(e);
		}
		this.otpPeer = new OtpPeer(this.peerNodeName.trim());
		
	}

}
