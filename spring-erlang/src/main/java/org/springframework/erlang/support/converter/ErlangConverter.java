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

package org.springframework.erlang.support.converter;

import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * Converter between Java and Erlang Types.  Additional support for converting results from RPC calls. 
 * 
 * @author Mark Pollack
 */
public interface ErlangConverter {

	/**
	 * Convert a Java object to a Erlang data type.
	 * @param object the object to convert
	 * @return the Erlang data type
	 * @throws ErlangConversionException in case of conversion failure
	 */
	OtpErlangObject toErlang(Object object) throws ErlangConversionException;

	/**
	 * Convert from a Erlang data type to a Java object.
	 * @param erlangObject the Elang object to convert
	 * @return the converted Java object
	 * @throws ErlangConversionException in case of conversion failure
	 */
	Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException;
	
	Object fromErlangRpc(String module, String function, OtpErlangObject erlangObject) throws ErlangConversionException;
}
