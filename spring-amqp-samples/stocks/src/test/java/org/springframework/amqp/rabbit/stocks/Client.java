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
package org.springframework.amqp.rabbit.stocks;


import javax.swing.JFrame;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 

import org.junit.Test;
import org.springframework.amqp.rabbit.stocks.ui.StockController;
import org.springframework.amqp.rabbit.stocks.ui.StockPanel;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Main client application, can run as app or unit test.
 * @author Mark Pollack
 *
 */
public class Client {

	private static Log log = LogFactory.getLog(Client.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Client().run();
	}
	
	@Test
	public void run() {
		
		ConfigurableApplicationContext ac =	new ClassPathXmlApplicationContext("client-bootstrap-config.xml");
		
		StockController controller = ac.getBean(StockController.class);
		
		JFrame f = new JFrame("Rabbit Stock Demo");
	    f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
	    
	    //TODO consider @Configurable
	    f.add(new StockPanel(controller));
	    f.pack();
	    f.setVisible(true);	    	
	}

	


}
