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

package org.springframework.otp.erlang.support.converter;

import java.util.ArrayList;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangByte;
import com.ericsson.otp.erlang.OtpErlangChar;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangFloat;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangPid;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangShort;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Converter that supports the basic types and arrays.
 * @author Mark Pollack
 * 
 */
public class SimpleErlangConverter implements ErlangConverter {

	public Object fromErlang(OtpErlangObject erlangObject)
			throws ErlangConversionException {
		//TODO support arrays.
		return convertErlangToBasicType(erlangObject);
	}

	public Object fromErlangRpc(String module, String function,
			OtpErlangObject erlangObject) throws ErlangConversionException {
		return this.fromErlang(erlangObject);
	}

	public OtpErlangObject toErlang(Object obj)
			throws ErlangConversionException {
		if (obj instanceof OtpErlangObject) {
			return (OtpErlangObject) obj;
		}
		if (obj instanceof Object[]) {
			Object[] objectsToConvert = (Object[]) obj;
			if (objectsToConvert.length != 0) {
				ArrayList<OtpErlangObject> tempList = new ArrayList<OtpErlangObject>();

				for (Object objectToConvert : objectsToConvert) {
					OtpErlangObject erlangObject = convertBasicTypeToErlang(objectToConvert);
					tempList.add(erlangObject);
				}
				OtpErlangObject ia[] = new OtpErlangObject[tempList.size()];
				return new OtpErlangList(tempList.toArray(ia));
			} else {
				return new OtpErlangList();
			}
		} else {
			return convertBasicTypeToErlang(obj);
		}
	}

	protected OtpErlangObject convertBasicTypeToErlang(Object obj) {
		if (obj instanceof byte[]) {
			return new OtpErlangBinary((byte[]) obj);
		} else if (obj instanceof Boolean) {
			return new OtpErlangBoolean((Boolean) obj);
		} else if (obj instanceof Byte) {
			return new OtpErlangByte((Byte) obj);
		} else if (obj instanceof Character) {
			return new OtpErlangChar((Character) obj);
		} else if (obj instanceof Double) {
			return new OtpErlangDouble((Double) obj);
		} else if (obj instanceof Float) {
			return new OtpErlangFloat((Float) obj);
		} else if (obj instanceof Integer) {
			return new OtpErlangInt((Integer) obj);
		} else if (obj instanceof Long) {
			return new OtpErlangLong((Long) obj);
		} else if (obj instanceof Short) {
			return new OtpErlangShort((Short) obj);
		} else if (obj instanceof String) {
			return new OtpErlangString((String) obj);
		} else {
			throw new ErlangConversionException(
					"Could not convert Java object of type [" + obj.getClass()
							+ "] to an Erlang data type.");
		}
	}

	protected Object convertErlangToBasicType(OtpErlangObject erlangObject) {
		try {
			if (erlangObject instanceof OtpErlangBinary) {
				return ((OtpErlangBinary) erlangObject).binaryValue();
			} else if (erlangObject instanceof OtpErlangAtom) {
				return ((OtpErlangAtom) erlangObject).atomValue();
			} else if (erlangObject instanceof OtpErlangBinary) {
				return ((OtpErlangBinary) erlangObject).binaryValue();
			} else if (erlangObject instanceof OtpErlangBoolean) {
				return extractBoolean(erlangObject);
			} else if (erlangObject instanceof OtpErlangByte) {
				return ((OtpErlangByte) erlangObject).byteValue();
			} else if (erlangObject instanceof OtpErlangChar) {
				return ((OtpErlangChar) erlangObject).charValue();
			} else if (erlangObject instanceof OtpErlangDouble) {
				return ((OtpErlangDouble) erlangObject).doubleValue();
			} else if (erlangObject instanceof OtpErlangFloat) {
				return ((OtpErlangFloat) erlangObject).floatValue();
			} else if (erlangObject instanceof OtpErlangInt) {
				return ((OtpErlangInt) erlangObject).intValue();
			} else if (erlangObject instanceof OtpErlangLong) {
				return ((OtpErlangLong) erlangObject).longValue();
			} else if (erlangObject instanceof OtpErlangShort) {
				return ((OtpErlangShort) erlangObject).shortValue();
			} else if (erlangObject instanceof OtpErlangString) {
				return ((OtpErlangString) erlangObject).stringValue();		
			} else if (erlangObject instanceof OtpErlangPid) {
				return ((OtpErlangPid) erlangObject).toString();		
			} else {
				throw new ErlangConversionException(
						"Could not convert Erlang object ["
								+ erlangObject.getClass() + "] to Java type.");
			}
		} catch (OtpErlangRangeException e) {
			throw new ErlangConversionException(
					"Could not convert Erlang object ["
							+ erlangObject.getClass() + "] to Java type.", e);
		}
	}

	public static boolean extractBoolean(OtpErlangObject erlangObject) {
		return ((OtpErlangBoolean) erlangObject).booleanValue();
	}
	
	public static String extractPid(OtpErlangObject value) {
		return ((OtpErlangPid)value).toString();
	}

	public static long extractLong(OtpErlangObject value) {
		return ((OtpErlangLong)value).longValue();
	}
}
