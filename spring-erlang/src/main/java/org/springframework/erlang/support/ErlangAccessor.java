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

package org.springframework.erlang.support;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.erlang.OtpException;
import org.springframework.erlang.connection.Connection;
import org.springframework.erlang.connection.ConnectionFactory;

import com.ericsson.otp.erlang.OtpAuthException;

/**
 * @author Mark Pollack
 */
public abstract class ErlangAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Log logger = LogFactory.getLog(getClass());

	private ConnectionFactory connectionFactory;
	
	protected Connection createConnection() throws UnknownHostException, OtpAuthException, IOException {
		return getConnectionFactory().createConnection();
	}
	
	public void setConnectionFactory(ConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}
	
	public ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}
	

	public void afterPropertiesSet() {
		if (getConnectionFactory() == null) {
			throw new IllegalArgumentException("Property 'connectionFactory' is required");
		}
	}
	
	protected OtpException convertOtpAccessException(Exception ex) {
		return ErlangUtils.convertOtpAccessException(ex);
	}
}
