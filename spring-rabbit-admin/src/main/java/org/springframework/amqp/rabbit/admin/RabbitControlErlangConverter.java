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

package org.springframework.amqp.rabbit.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.otp.erlang.core.Application;
import org.springframework.otp.erlang.core.Node;
import org.springframework.otp.erlang.support.converter.ErlangConversionException;
import org.springframework.otp.erlang.support.converter.ErlangConverter;
import org.springframework.otp.erlang.support.converter.SimpleErlangConverter;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

/***
 * Converter that understands the responses from the rabbit control module and related functionality.
 * 
 * @author Mark Pollack
 * @author Mark Fisher
 */
public class RabbitControlErlangConverter extends SimpleErlangConverter implements ErlangConverter {

	
	protected final Log logger = LogFactory.getLog(getClass());
	
	
	private Map<String, ErlangConverter> converterMap = new HashMap<String, ErlangConverter>();
	
	
	public RabbitControlErlangConverter() {
		initializeConverterMap();
	}


	public Object fromErlangRpc(String module, String function,  OtpErlangObject erlangObject) throws ErlangConversionException {
		ErlangConverter converter = getConverter(module, function);
		if (converter != null) {
			return converter.fromErlang(erlangObject);
		} else {
			return super.fromErlangRpc(module, function, erlangObject);
		}
	}


	protected ErlangConverter getConverter(String module, String function) {
		return converterMap.get(generateKey(module, function));	
	}


	protected void initializeConverterMap() {		
		registerConverter("rabbit_access_control", "list_users", new ListUsersConverter());	
		registerConverter("rabbit", "status", new StatusConverter());
		registerConverter("rabbit_amqqueue", "info_all", new InfoAllConverter());
	}
	
	protected void registerConverter(String module, String function, ErlangConverter listUsersConverter) {
		converterMap.put(generateKey(module, function), listUsersConverter);		
	}

	protected String generateKey(String module, String function) {
		return module + "%" + function;
	}

	public class ListUsersConverter extends SimpleErlangConverter {
		
		public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {
						
			List<String> users = new ArrayList<String>();
			if (erlangObject instanceof OtpErlangList) {
				OtpErlangList erlangList = (OtpErlangList) erlangObject;
				for (OtpErlangObject obj : erlangList) {
					if (obj instanceof OtpErlangBinary) {
						OtpErlangBinary binary = (OtpErlangBinary) obj;
						users.add(new String(binary.binaryValue()));
					}
				}
			}
			return users;
		}		
	}
	
	public class StatusConverter extends SimpleErlangConverter {
		
		public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {
			
			List<Application> applications = new ArrayList<Application>();
			List<Node> nodes = new ArrayList<Node>();
			List<Node> runningNodes = new ArrayList<Node>();
			if (erlangObject instanceof OtpErlangList) {
				OtpErlangList erlangList = (OtpErlangList) erlangObject;

				OtpErlangTuple runningAppTuple = (OtpErlangTuple)erlangList.elementAt(0);				
				OtpErlangList appList = (OtpErlangList)runningAppTuple.elementAt(1);
				extractApplications(applications, appList);
				
				OtpErlangTuple nodesTuple = (OtpErlangTuple)erlangList.elementAt(1);
				OtpErlangList nodesList = (OtpErlangList)nodesTuple.elementAt(1);
				extractNodes(nodes, nodesList);
				
							
				OtpErlangTuple runningNodesTuple  = (OtpErlangTuple)erlangList.elementAt(2);
				nodesList = (OtpErlangList)runningNodesTuple.elementAt(1);
				extractNodes(runningNodes, nodesList);
				
				/*
				for (OtpErlangObject obj : erlangList) {
					if (obj instanceof OtpErlangBinary) {
						OtpErlangBinary binary = (OtpErlangBinary) obj;
						users.add(new String(binary.binaryValue()));
					}
				}*/
			}
			
			return new RabbitStatus(applications, nodes, runningNodes);
		}

		private void extractNodes(List<Node> nodes, OtpErlangList nodesList) {
			for (OtpErlangObject erlangNodeName : nodesList) {
				String nodeName = erlangNodeName.toString();
				nodes.add(new Node(nodeName));
			}			
		}

		private void extractApplications(List<Application> applications, OtpErlangList appList) {
			for (OtpErlangObject appDescription : appList) {
				OtpErlangTuple appDescriptionTuple = (OtpErlangTuple)appDescription;
				String name = appDescriptionTuple.elementAt(0).toString();
				String description = appDescriptionTuple.elementAt(1).toString();
				String version = appDescriptionTuple.elementAt(2).toString();
				applications.add(new Application(name, description, version));
			}
		}	
	}

	public class InfoAllConverter extends SimpleErlangConverter {

		@Override
		public Object fromErlang(OtpErlangObject erlangObject) throws ErlangConversionException {
			Map<String, Map<String, String>> allQueuesMap = new HashMap<String, Map<String,String>>();
			if (erlangObject instanceof OtpErlangList) {
				OtpErlangList erlangList = (OtpErlangList) erlangObject;
				for (OtpErlangObject element : erlangList.elements()) {
					String queueName = null;
					Map<String, String> queueMap = new HashMap<String, String>();
					OtpErlangList itemList = (OtpErlangList) element;
					for (OtpErlangObject item : itemList.elements()) {
						OtpErlangTuple tuple = (OtpErlangTuple) item;
						if (tuple.arity() == 2) {
							String key = tuple.elementAt(0).toString();
							Object value = tuple.elementAt(1);
							if ("name".equals(key)) {
								// arity should be 4 for the 'name' tuple, we want the last element
								Object nameElement = ((OtpErlangTuple) value).elementAt(3);
								queueName = new String(((OtpErlangBinary) nameElement).binaryValue());
								value = queueName;
							}
							queueMap.put(tuple.elementAt(0).toString(), value.toString());
						}
					}
					if (queueMap != null) {
						allQueuesMap.put(queueName, queueMap);
					}
				}
			}
			return allQueuesMap;
		}
	}

}
