package org.deri.hcls.metex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.jena.riot.RiotException;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.ResourceHelper;
import org.deri.hcls.vocabulary.VOIDX;
import org.deri.hcls.vocabulary.Vocabularies;

import virtuoso.jena.driver.VirtModel;

import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class DescriberAndExtractor {

	private Set<Endpoint> endpoints = new HashSet<Endpoint>();
	private Model model;
	private Map<String, ExtractorServiceAdapter> adapterRegistry = new HashMap<String, ExtractorServiceAdapter>();

	public static void main(String args[]) {
		DescriberAndExtractor describerAndExtractor = new DescriberAndExtractor();

		if (args.length > 0) {
			String endpointUri = args[0];
			describerAndExtractor.addEndpoint(endpointUri);
		} else {
			describerAndExtractor.fetchListOfEndpoints();
		}
		describerAndExtractor.run();
	}

	public DescriberAndExtractor() {
		/*
		 * create a persistent model so we can store it for further requests
		 */
		model = VirtModel.openDatabaseModel(
				"http://metex.hcls.deri.org/",
				"jdbc:virtuoso://localhost:1111", "dba", "dba");
	}

	public void addEndpoint(String endpointUri) {
		Endpoint endpoint = new Endpoint(endpointUri, model);
		if (endpoints.contains(endpoint)) {
			System.err.println("duplicate");
		}
		endpoints.add(endpoint);
	}

	public void addAllEndpoints(Collection<String> endpointUris) {
		for (String endpointUri : endpointUris) {
			addEndpoint(endpointUri);
		}
	}

	public void fetchListOfEndpoints() {
		Collection<EndpointListProviderAdapter> services = new ArrayList<EndpointListProviderAdapter>();
		services.add(new DatahubAdapter());
		services.add(new VoidStoreAdapter());

		for (EndpointListProviderAdapter adapter : services) {

			try {
				Collection<String> adapterEndpoints = adapter
						.getAllEndpoints();
				addAllEndpoints(adapterEndpoints);

				System.out.println("We got " + adapterEndpoints.size()
						+ " endpoints from " + adapter.getClass().getName()
						+ ", now we have " + endpoints.size() + " in total");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void run() {

		/*
		 * TODO provide a mechanism to also handle single endpoints
		 */

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
				statusResource.addLiteral(VOIDX.httpReturnCode,
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
			 * get metadata from endpoint for describing the endpoint and its
			 * datasets
			 */
			Model endpointMetadata = endpoint.getMetadata();
			Model extractedMetadata = endpoint.extractMetadata();

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

			/*
			 * if still metadata is missing, try to request it from external
			 * services
			 */


			System.err.println("Start to get datahub metadata …");
			ExtractorServiceAdapter datahubAdapter = getAdapter("datahub");
			try {

				System.err.println("try get metadata …");
				Model metadata = datahubAdapter.getMetadata(endpoint.getUri());
				System.err.println("ready get metadata …");
				System.err.println("write metadata …");

				if (writeToVirtuoso(metadata, endpointResource)) {
					System.err.println("Writng datahub metadata for "
							+ endpoint.getUri() + " … done");
					statusResource.addProperty(VOIDX.availableAt,
							datahubAdapter.getServiceUri());
				} else {
					System.err
							.println("No datahub metadata was found for endpoint "
									+ endpoint.getUri());
					statusResource.addProperty(VOIDX.unavailableAt,
							datahubAdapter.getServiceUri());
				}
			} catch (IOException e) {
				System.err
						.println("Couldn't get datahub metadata for endpoint: "
								+ endpoint.getUri());
				e.printStackTrace();
				statusResource.addProperty(VOIDX.unavailableAt,
						datahubAdapter.getServiceUri());
			}

			System.err.println("Start to get voidstore metadata …");
			ExtractorServiceAdapter voidstoreAdapter = getAdapter("voidstore");
			
			try {
				Model metadata = voidstoreAdapter
						.getMetadata(endpoint.getUri(), Endpoint.datasetFetchProperties);

				if (writeToVirtuoso(metadata, endpointResource)) {
					System.err.println("Writng voidstore metadata for "
							+ endpoint.getUri() + " … done");
					statusResource.addProperty(VOIDX.availableAt,
							voidstoreAdapter.getServiceUri());
				} else {
					System.err
							.println("No voidstore metadata was found for endpoint "
									+ endpoint.getUri());
					statusResource.addProperty(VOIDX.unavailableAt,
							voidstoreAdapter.getServiceUri());
				}
			} catch (IOException e) {
				System.err
						.println("Couldn't get voidstore metadata for endpoint: "
								+ endpoint.getUri());
				e.printStackTrace();
				statusResource.addProperty(VOIDX.unavailableAt,
						voidstoreAdapter.getServiceUri());
			}

			System.err.println("Start to get lodstats metadata …");
			ExtractorServiceAdapter lodstatsAdapter = getAdapter("lodstats");
			try {
				Model metadata = lodstatsAdapter.getMetadata(endpoint.getUri());

				if (writeToVirtuoso(metadata, endpointResource)) {
					System.err.println("Writng lodstats metadata for "
							+ endpoint.getUri() + " … done");
					statusResource.addProperty(VOIDX.availableAt,
							lodstatsAdapter.getServiceUri());
				} else {
					System.err
							.println("No lodstats metadata was found for endpoint "
									+ endpoint.getUri());
					statusResource.addProperty(VOIDX.unavailableAt,
							lodstatsAdapter.getServiceUri());
				}
			} catch (IOException e) {
				System.err
						.println("Couldn't get lodstats metadata for endpoint: "
								+ endpoint.getUri());
				e.printStackTrace();
				statusResource.addProperty(VOIDX.unavailableAt,
						lodstatsAdapter.getServiceUri());
			}

			System.err.println("Start to get sindice summary …");
			ExtractorServiceAdapter sindiceAdapter = getAdapter("sindice");
			try {
				sindiceAdapter.getMetadata(endpoint.getUri());
			} catch (IOException e) {
				System.err
						.println("Couldn't get sindice summary for endpoint: "
								+ endpoint.getUri());
			}

			/*
			 * if still metadata is missing, try to extract metadata by running
			 * a bunch of queries
			 */

			System.err
					.println("Start to manually extract the missing statistics …");
			ExtractorServiceAdapter manualExtractor = getAdapter("manualextractor");
			try {
				manualExtractor.getMetadata(endpoint,
						Endpoint.endpointProperties);
			} catch (IOException e) {
				System.err.println("Couldn't extract data for endpoint: "
						+ endpoint.getUri());
				e.printStackTrace();
			}

		} else {
			System.err.println("Not available");
		}
	}

	private ExtractorServiceAdapter getAdapter(String adapterName) {
		if (!adapterRegistry.containsKey(adapterName)) {
			ExtractorServiceAdapter adapter;

			if (adapterName.toLowerCase().equals("sindice")) {
				adapter = new SindiceAdapter(model);
			} else if (adapterName.toLowerCase().equals("datahub")) {
				adapter = new DatahubAdapter();
			} else if (adapterName.toLowerCase().equals("voidstore")) {
				adapter = new VoidStoreAdapter();
			} else if (adapterName.toLowerCase().equals("lodstats")) {
				adapter = new LODStatsAdapter();
			} else if (adapterName.toLowerCase().equals("manualextractor")) {
				adapter = new ManualExtractorAdapter(model);
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
						Statement stmt = statements.next();
						System.err.println("next: " + stmt.getSubject().getURI() + " " + stmt.getPredicate().getURI() + " " + stmt.getObject().toString());
						this.model.add(stmt);
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
