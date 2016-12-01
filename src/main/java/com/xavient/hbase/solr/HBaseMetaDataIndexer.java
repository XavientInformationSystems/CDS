package com.xavient.hbase.solr;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import com.xavient.hdfshbase.constants.Constants;

public class HBaseMetaDataIndexer {
    private static final String URL_STRING = "http://localhost:8983/solr/hbase";

    private SolrClient solr;

    public HBaseMetaDataIndexer() {
	super();
	solr = new HttpSolrClient.Builder(URL_STRING).build();
	((HttpSolrClient) solr).setParser(new XMLResponseParser());
    }

    public static void main(String[] args) throws SolrServerException, IOException {
	HBaseMetaDataIndexer metaDataIndexer = new HBaseMetaDataIndexer();
	metaDataIndexer.search();
    }

    public SolrDocumentList search() throws SolrServerException, IOException {
	SolrQuery query = new SolrQuery();
	query.setQuery("*:*");
	QueryResponse response = solr.query(query);
	SolrDocumentList documentList = response.getResults();
	System.out.println(documentList);
	return documentList;
    }

    public void indexDocument(String rowid, String result) throws SolrServerException, IOException {
	SolrInputDocument document = new SolrInputDocument();
	document.addField(Constants.ID, rowid);
	document.addField(Constants.CONTENT, result);
	UpdateResponse response = solr.add(document);
	// Remember to commit your changes!
	solr.commit();
	System.out.println(response);
    }

}
