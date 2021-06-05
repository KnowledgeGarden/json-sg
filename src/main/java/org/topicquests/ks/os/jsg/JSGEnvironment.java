/**
 * Copyright 2021, TopicQuests Foundation
 *  This source code is available under the terms of the Affero General Public License v3.
 *  Please see LICENSE.txt for full license terms, including the availability of proprietary exceptions.
 */

package org.topicquests.ks.os.jsg;

import org.topicquests.ks.kafka.KafkaProducer;
import org.topicquests.ks.os.jsg.graph.api.IDataProvider;
import org.topicquests.ks.os.jsg.graph.api.IProxyModel;
import org.topicquests.ks.os.jsg.graph.api.ITupleQuery;
import org.topicquests.ks.os.jsg.graph.api.IVirtualizer;
import org.topicquests.ks.os.jsg.graph.merge.DefaultVirtualizer;
import org.topicquests.ks.os.jsg.graph.merge.VirtualizerHandler;
import org.topicquests.support.RootEnvironment;

/**
 * @author jackpark
 *
 */
public class JSGEnvironment extends RootEnvironment {
	private VirtualizerHandler virtualizerHandler = null;
	private IVirtualizer virtualizer;
	//data
	private IDataProvider dataProvider = null;
	//kafka
	private KafkaProducer kProducer = null ;
	// queries
	protected ITupleQuery tupleQuery;
	// models
	private IProxyModel proxyModel;



	/**
	 */
	public JSGEnvironment() {
		super("subjectgraph-props.xml", "logger.properties");
		
	    try {
	        virtualizerHandler = new VirtualizerHandler(this);
	        virtualizer = new DefaultVirtualizer();
	        // TODO virtualizer.init(this);
	        // TODO dataProvider = new DataProvider(this);
	        // TODO tupleQuery = new TupleQuery(this, dataProvider);
	        // proxyModel = new ProxyModel(this, dataProvider);

	  	  String clientId = "tmProducer"+Long.toString(System.currentTimeMillis());

	      kProducer = new KafkaProducer(this, clientId);
	    } catch (Exception e) {
	        logError(e.getMessage(), e);
	        e.printStackTrace();
	        shutDown();
	        System.exit(1);
	    }
	}
	
	public VirtualizerHandler getVirtualizerHandler() {
		return virtualizerHandler;
	}
	
	public IVirtualizer getVirtualizer() {
		return virtualizer;
	}

	public ITupleQuery getTupleQuery() {
		return tupleQuery;
	}
	/**
	 * @return Kafka producer
	*/
	public KafkaProducer getkafkaProducer() {
		return kProducer;
	}
	public IDataProvider getDataProvider() {
		return dataProvider;
	}
	public IProxyModel getProxyModel() {
		return proxyModel;
	}



	@Override
	public void shutDown() {
		System.out.println("Shutting down");
	    virtualizerHandler.shutDown();
	    try {
	      //eventHandler.shutDown();
	      kProducer.close();
	      //esProvider.shutDown();
	      
	      if (dataProvider != null)
	        dataProvider.shutDown();
	    } catch (Exception e) {
	      logError(e.getMessage(),e);
	    }
	}

}
