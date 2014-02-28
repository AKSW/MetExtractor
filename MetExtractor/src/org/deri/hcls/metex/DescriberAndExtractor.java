package org.deri.hcls.metex;

import ie.deri.hcls.QueryExecutionException;
import ie.deri.hcls.ResourceHelper;
import ie.deri.hcls.vocabulary.VOIDX;
import ie.deri.hcls.vocabulary.Vocabularies;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.riot.RiotException;

import virtuoso.jena.driver.VirtModel;

import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class DescriberAndExtractor {

	private Set<Endpoint> endpoints = new HashSet<Endpoint>();
	private Model model;
	private Map<String, WebServiceAdapter> adapterRegistry = new HashMap<String, WebServiceAdapter>();

	public static void main(String args[]) {
		DescriberAndExtractor describerAndExtractor = new DescriberAndExtractor();

		describerAndExtractor.run();
	}

	public DescriberAndExtractor() {
		/*
		 * TODO create a persistent model so we can store it for further
		 * requests
		 */
		// model = ModelFactory.createDefaultModel();
		model = VirtModel.openDatabaseModel(
				"http://hcls.deri.ie/endpointcatalog/",
				"jdbc:virtuoso://localhost:1111", "dba", "dba");

		// model = new VirtModel(virtGraph);
	}

	public void addEndpoint(String endpointUri) {
		Endpoint endpoint = new Endpoint(endpointUri, model);
		endpoints.add(endpoint);
	}

	public void addAllEndpoints(Collection<String> endpointUris) {
		for (String endpointUri : endpointUris) {
			addEndpoint(endpointUri);
		}
	}

	public void run() {

		/*
		 * TODO provide a mechanism to also handle single endpoints
		 */

		WebServiceAdapter service = getAdapter("datahub");

		try {
			addAllEndpoints(service.getAllEndpoints());
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("We got " + endpoints.size()
				+ " endpoints from datahub");

		service = getAdapter("voidstore");

		try {
			addAllEndpoints(service.getAllEndpoints());
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("We have " + endpoints.size()
				+ " endpoints after also asking voidStore");

		/*
		 * TODO see if we can run this in multiple threads
		 */
		for (Endpoint endpoint : endpoints) {
			System.err.println("Get metadata for endpoint: "
					+ endpoint.getUri());
			runForEndpoint(endpoint);

			model.commit();
		}

		System.err.println("Done");
	}

	private void runForEndpoint(Endpoint endpoint) {
		Resource endpointResource = endpoint.getEndpointResource();

		endpointResource.addProperty(RDF.type, Vocabularies.sd_Service);

		boolean available = false;

		Resource statusResource = ResourceHelper.createRandomResource(model);
		statusResource.addProperty(RDF.type, Vocabularies.ENDS_STATUS);
		statusResource.addProperty(DCTerms.date,
				ResourceHelper.getCurrentTimeStamp());
		try {
			// This will throw an exception, if the endpoint is not available
			available = endpoint.isAvailable();

		} catch (QueryExecutionException e) {
			statusResource.addLiteral(RDFS.comment, e.toString());
			if (e.getCause() instanceof QueryExceptionHTTP) {
				QueryExceptionHTTP httpE = (QueryExceptionHTTP) e.getCause();
				statusResource.addLiteral(VOIDX.HTTP_RETURN_CODE,
						httpE.getResponseCode());
			}
			available = false;
		} catch (QueryException e) {
			statusResource.addLiteral(RDFS.comment,
					"This endpoint is not available and we got a QueryException with message: "
							+ e.getMessage());
			available = false;
		} catch (RiotException e) {
			statusResource.addLiteral(
					RDFS.comment,
					"The endpoint returned invalide RDF. Exception: "
							+ e.getMessage());
			available = false;
		} catch (Exception e) {
			System.err.println("Caut an exception of type: "
					+ e.getClass().getName());
			e.printStackTrace();
			available = false;
		}

		endpointResource.addProperty(Vocabularies.ENDS_STATUS_PROP,
				statusResource);
		statusResource.addLiteral(Vocabularies.ENDS_STATUS_IS_AVAILABLE_PROP,
				available);

		if (available) {
			/*
			 * get metadata from endpoint
			 */
			Model endpointMetadata = endpoint.getMetadata();

			/*
			 * TODO check which metadata we got so far
			 */

			/*
			 * TODO if still metadata is missing, try to extract metadata by
			 * running a bunch of queries
			 */

			Model extractedMetadata = endpoint.extractMetadata();

			/*
			 * TODO check which metadata we got so far
			 */

			/*
			 * TODO if still metadata is missing, try to request it from
			 * external services
			 */
			System.err.println("Start to get sindice summary …");
			WebServiceAdapter sindiceAdapter = getAdapter("sindice");
			try {
				sindiceAdapter.getMetadata(endpoint.getUri());
			} catch (IOException e) {
				System.err
						.println("Couldn't get sindice summary for endpoint: "
								+ endpoint.getUri());
			}

			if (writeToVirtuoso(endpointMetadata, endpointResource)) {
				System.err.println("Writng LD for " + endpoint.getUri()
						+ " … done");
			} else {
				System.err.println("No LD metadata was found for endpoint "
						+ endpoint.getUri());
			}

			if (writeToVirtuoso(extractedMetadata, endpointResource)) {
				System.err.println("Writng extracted metadata for "
						+ endpoint.getUri() + " … done");
			} else {
				System.err
						.println("No extracted metadata was found for endpoint "
								+ endpoint.getUri());
			}

		} else {
			System.err.println("Not available");
		}
	}

	private WebServiceAdapter getAdapter(String adapterName) {
		if (!adapterRegistry.containsKey(adapterName)) {
			WebServiceAdapter adapter;

			if (adapterName.toLowerCase().equals("sindice")) {
				adapter = new SindiceAdapter(model);
			} else if (adapterName.toLowerCase().equals("datahub")) {
				adapter = new DatahubAdapter();
			} else if (adapterName.toLowerCase().equals("voidstore")) {
				adapter = new VoidStoreAdapter();
			} else {
				adapter = null;
			}

			adapterRegistry.put(adapterName, adapter);
		}
		return adapterRegistry.get(adapterName);
	}

	private boolean writeToVirtuoso(Model modelToWrite,
			Resource endpointResource) {
		if (modelToWrite.isEmpty()) {
			System.err.println("model is empty");
			return false;
		} else {
			try {
				this.model.add(modelToWrite);
			} catch (AddDeniedException e) {
				System.err
						.println("error writing to virtuoso, try it for each single statement");

				StmtIterator statements = modelToWrite.listStatements();

				while (statements.hasNext()) {
					try {
						this.model.add(statements.next());
					} catch (AddDeniedException ee) {
						endpointResource.addLiteral(RDFS.comment,
								"Error when adding gathered statements to virtuoso model: "
										+ e.getMessage());
					}
				}
			}
			return true;
		}
	}
}
