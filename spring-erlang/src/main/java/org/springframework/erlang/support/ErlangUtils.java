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

import org.springframework.erlang.OtpException;
import org.springframework.erlang.OtpIOException;
import org.springframework.erlang.UncategorizedOtpException;
import org.springframework.util.Assert;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpConnection;

/**
 * @author Mark Pollack
 */
public class ErlangUtils {

	/**
	 * Close the given Connection.
	 * @param con the Connection to close if necessary
	 * (if this is <code>null</code>, the call will be ignored)
	 */
	public static void releaseConnection(OtpConnection con) {
		if (con == null) {
			return;
		}
		con.close();		
	}
	
	public static OtpException convertOtpAccessException(Exception ex) {
		Assert.notNull(ex, "Exception must not be null");
		if (ex instanceof IOException) {
			return new OtpIOException((IOException) ex);
		}		
		if (ex instanceof OtpAuthException) {
			return new org.springframework.erlang.OtpAuthException((OtpAuthException) ex);
		}		
		//fallback
		return new UncategorizedOtpException(ex);
	}
}
