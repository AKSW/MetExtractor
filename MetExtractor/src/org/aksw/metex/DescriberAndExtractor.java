package org.aksw.metex;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.metex.adapters.DatahubAdapter;
import org.aksw.metex.adapters.GuessLDAdapter;
import org.aksw.metex.adapters.LODStatsAdapter;
import org.aksw.metex.adapters.LinkedDataAdapter;
import org.aksw.metex.adapters.ManualExtractorAdapter;
import org.aksw.metex.adapters.SindiceAdapter;
import org.aksw.metex.adapters.VoidStoreAdapter;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.math.RandomUtils;
import org.apache.jena.riot.RiotException;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.deri.hcls.Configuration;
import org.deri.hcls.Configuration.VirtuosoConfiguration;
import org.deri.hcls.HelpException;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.ResourceHelper;
import org.deri.hcls.vocabulary.VOIDX;
import org.deri.hcls.vocabulary.Vocabularies;

import virtuoso.jdbc4.VirtuosoDataSource;
import virtuoso.jena.driver.VirtModel;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.shared.AddDeniedException;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.OWL;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;
import com.thetransactioncompany.jsonrpc2.JSONRPC2Request;
import com.thetransactioncompany.jsonrpc2.JSONRPC2Response;
import com.thetransactioncompany.jsonrpc2.client.JSONRPC2Session;
import com.thetransactioncompany.jsonrpc2.client.JSONRPC2SessionException;

public class DescriberAndExtractor {
	public static final String CONF_NS = "http://metex.hcls.deri.org/voc/conf/";
	private static OntModel ontModel = ModelFactory.createOntologyModel(
			OntModelSpec.OWL_MEM, null);
	public static final Property CONF_baseUri = ontModel.createProperty(CONF_NS
			+ "baseUri");
	public static final Property CONF_owBaseUri = ontModel
			.createProperty(CONF_NS + "owBaseUri");
	public static final Property CONF_owUser = ontModel.createProperty(CONF_NS
			+ "owUser");
	public static final Property CONF_owPassword = ontModel
			.createProperty(CONF_NS + "owPassword");
	public static final Property CONF_endpointsFetchLimit = ontModel
			.createProperty(CONF_NS + "endpointFetchLimit");
	public static final Property CONF_endpointsPrintLists = ontModel
			.createProperty(CONF_NS + "endpointsPrintLists");
	public static final Property CONF_endpoint = ontModel
			.createProperty(CONF_NS + "endpoint");

	private VirtuosoDataSource ds;
	private Model siteModel;
	private String baseNs;
	private Set<Endpoint> endpoints = new HashSet<Endpoint>();
	private Map<String, ExtractorServiceAdapter> adapterRegistry = new HashMap<String, ExtractorServiceAdapter>();
	private Map<String, Model> models = new HashMap<String, Model>();
	private Collection<ExtractorServiceAdapter> services;
	private Configuration config;

	public static void main(String args[]) {
		try {
			Configuration config = new Configuration(args);

			/*
			 * Check if endpoint file exists
			 */
			/*
			 * File endpointFile = config.getEndpointFile(); if
			 * (!endpointFile.exists()) { System.err.println("The given file \""
			 * + endpointFile + "\" does not exist!"); return; }
			 */

			DescriberAndExtractor describerAndExtractor = new DescriberAndExtractor(
					config);

			describerAndExtractor.run();
		} catch (HelpException e) {
			// Do nothing. The help message was requested.
		} catch (Exception e) {
			System.err.println("Coud not extract data.");

			System.err.println(e.getMessage());
			e.printStackTrace();
		}
	}

	public DescriberAndExtractor(Configuration config) {
		this.config = config;

		/*
		 * configure Log4J to be used by Jena
		 */
		Appender logAppender = new ConsoleAppender(new PatternLayout(),
				ConsoleAppender.SYSTEM_ERR);
		BasicConfigurator.configure(logAppender);
		if (config.verbose()) {
			LogManager.getRootLogger().setLevel(Level.DEBUG);
		} else {
			LogManager.getRootLogger().setLevel(Level.ERROR);
		}

		this.baseNs = config.getPropertyAsString(CONF_baseUri,
				"http://metex.hcls.deri.org/");

		/*
		 * create a persistent model so we can store it for further requests
		 */

		VirtuosoConfiguration virtDsConf = config.getVirtuosoConfig();

		ds = new VirtuosoDataSource();
		ds.setServerName(virtDsConf.serverName);
		ds.setPortNumber(virtDsConf.portNumber);
		ds.setUser(virtDsConf.user);
		ds.setPassword(virtDsConf.password);
		siteModel = VirtModel.openDatabaseModel(baseNs, ds);
	}

	public Model getModelFor(ExtractorServiceAdapter adapter) {
		return getModelFor(adapter.getClass().getSimpleName());
	}

	public Model getModelFor(String adapterName) {
		String modelUri = baseNs + adapterName + "/";
		if (!models.containsKey(modelUri)) {
			Model adapterModel = VirtModel.openDatabaseModel(modelUri, ds);
			Resource siteModelResource = siteModel.createResource(baseNs);
			Resource adapterModelResource = adapterModel
					.createResource(modelUri);
			siteModelResource.addProperty(OWL.imports, adapterModelResource);
			adapterModelResource.addProperty(RDFS.label, "MetEx: "
					+ adapterName);
			models.put(modelUri, adapterModel);
		}
		return models.get(modelUri);
	}

	public void addEndpoint(String endpointUri) {
		if (!endpointUri.equals("")) {
			Endpoint endpoint = new Endpoint(endpointUri, siteModel);
			endpoints.add(endpoint);
		}
	}

	public void addAllEndpoints(Collection<String> endpointUris) {
		for (String endpointUri : endpointUris) {
			addEndpoint(endpointUri);
		}
	}

	public void fetchListOfEndpoints() {

		int limit = config.getPropertyAsInt(CONF_endpointsFetchLimit, -1);
		boolean printLists = config.getPropertyAsBoolen(
				CONF_endpointsPrintLists, false);

		Collection<EndpointListProviderAdapter> services = new ArrayList<EndpointListProviderAdapter>();
		services.add(new DatahubAdapter());
		services.add(new VoidStoreAdapter(Endpoint.datasetFetchProperties,
				Endpoint.termSubstitutions));

		for (EndpointListProviderAdapter adapter : services) {
			try {
				Collection<String> adapterEndpoints;
				if (limit < 0) {
					adapterEndpoints = adapter.getAllEndpoints();
				} else {
					adapterEndpoints = adapter.getSomeEndpoints(limit);
				}
				addAllEndpoints(adapterEndpoints);

				System.out.println("We got " + adapterEndpoints.size()
						+ " endpoints from "
						+ adapter.getClass().getSimpleName() + ", now we have "
						+ endpoints.size() + " in total");

				if (printLists) {
					for (String endpointUri : adapterEndpoints) {
						System.out.println(endpointUri);
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void run() {

		String endpointUri = config.getPropertyAsString(CONF_endpoint, null);

		System.err.println("The configured endpoint is: " + endpointUri);

		if (endpointUri != null) {
			/*
			 * configure a single endpoint
			 */
			addEndpoint(endpointUri);
		} else {
			/*
			 * Fetch a list of endpoints
			 */
			fetchListOfEndpoints();
		}

		services = new ArrayList<ExtractorServiceAdapter>();
		services.add(getAdapter("linkeddata"));
		services.add(getAdapter("guessld"));
		services.add(getAdapter("datahub"));
		services.add(getAdapter("voidstore"));
		services.add(getAdapter("lodstats"));
		services.add(getAdapter("sindice"));
		services.add(getAdapter("manualextractor"));

		for (ExtractorServiceAdapter service : services) {
			getModelFor(service);
		}

		registerModelsAtOntoWiki();

		/*
		 * TODO see if we can run this in multiple threads
		 */
		int numOfEndpoints = endpoints.size();
		int i = 0;
		for (Endpoint endpoint : endpoints) {
			i++;
			System.err.println(i + "/" + numOfEndpoints
					+ ": Get metadata for endpoint: " + endpoint.getUri());
			runForEndpoint(endpoint);

			siteModel.commit();
		}

		System.err.println("Done");
	}

	private void runForEndpoint(Endpoint endpoint) {
		Resource endpointResource = endpoint.getEndpointResource();

		endpointResource.addProperty(RDF.type, Vocabularies.SD_Service);

		boolean available = false;

		String timeStamp = ResourceHelper.getCurrentTimeStamp();
		Resource statusResource = ResourceHelper
				.createRandomResource(siteModel);
		statusResource.addProperty(RDF.type, Vocabularies.ENDS_STATUS);
		statusResource.addProperty(DCTerms.date, timeStamp);
		statusResource.addProperty(RDFS.label, "Status of " + endpoint.getUri()
				+ " @ " + timeStamp);
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
					+ e.getClass().getSimpleName());
			e.printStackTrace();
			available = false;
		}

		endpointResource.addProperty(Vocabularies.ENDS_STATUS_PROP,
				statusResource);
		statusResource.addLiteral(Vocabularies.ENDS_STATUS_IS_AVAILABLE_PROP,
				available);

		if (available) {
			int i = 0;
			int num = services.size();
			String serviceName, retMessage;
			int success;
			System.err.println("Will get data from " + num + " services");
			for (ExtractorServiceAdapter service : services) {
				i++;
				serviceName = service.getClass().getSimpleName();
				Resource serviceLink = siteModel.createResource(service
						.getServiceLink(endpoint.getUri()));
				endpointResource.addProperty(RDFS.seeAlso, serviceLink);

				System.err.print(i + "/" + num + " (" + serviceName + "): ");

				if (!service.isAvailable()) {
					System.err.println(" … unavailable.");
					continue;
				}

				Model adapterModel = getModelFor(service);

				success = -1;
				System.err.println("start getting metadata.");

				try {
					Model metadata = service.getMetadata(endpoint.getUri());
					if (metadata != null) {
						if (writeToVirtuoso(metadata, endpointResource,
								adapterModel)) {
							success = 1;
						}
					} else {
						success = 0;
					}
				} catch (IOException e) {
					System.err.println("Couldn't get metadata for endpoint: "
							+ endpoint.getUri());
					e.printStackTrace();
					success = -2;
				}

				Resource serviceResource = siteModel.createResource(service
						.getServiceUri());
				if (success > 0) {
					statusResource.addProperty(VOIDX.availableAt,
							serviceResource);
					retMessage = "successful";
				} else if (success < 0) {
					statusResource.addProperty(VOIDX.unavailableAt,
							serviceResource);
					retMessage = "failed";
				} else {
					retMessage = "done or nothing available";
				}

				System.err.println("Getting and writing metadata from "
						+ serviceName + " for " + endpoint.getUri() + " … "
						+ retMessage);
			}
		} else {
			System.err.println("Not available");
		}
	}

	private ExtractorServiceAdapter getAdapter(String adapterName) {
		if (!adapterRegistry.containsKey(adapterName)) {
			ExtractorServiceAdapter adapter;

			if (adapterName.toLowerCase().equals("sindice")) {
				Model sindiceModel = getModelFor(SindiceAdapter.class
						.getSimpleName());
				adapter = new SindiceAdapter(sindiceModel);
			} else if (adapterName.toLowerCase().equals("datahub")) {
				adapter = new DatahubAdapter();
			} else if (adapterName.toLowerCase().equals("voidstore")) {
				adapter = new VoidStoreAdapter(Endpoint.datasetFetchProperties,
						Endpoint.termSubstitutions);
			} else if (adapterName.toLowerCase().equals("lodstats")) {
				adapter = new LODStatsAdapter();
			} else if (adapterName.toLowerCase().equals("linkeddata")) {
				adapter = new LinkedDataAdapter(
						Endpoint.endpointFetchProperties,
						Endpoint.datasetFetchProperties,
						Endpoint.termSubstitutions);
			} else if (adapterName.toLowerCase().equals("guessld")) {
				adapter = new GuessLDAdapter(Endpoint.endpointFetchProperties,
						Endpoint.termSubstitutions);
			} else if (adapterName.toLowerCase().equals("manualextractor")) {
				adapter = new ManualExtractorAdapter(
						Endpoint.endpointProperties);
			} else {
				adapter = null;
			}

			adapterRegistry.put(adapterName, adapter);
		}
		return adapterRegistry.get(adapterName);
	}

	private boolean writeToVirtuoso(Model modelToWrite,
			Resource endpointResource, Model writeTo) {
		if (modelToWrite.isEmpty()) {
			System.err.println("model is empty");
			return false;
		} else {
			try {
				writeTo.add(modelToWrite);
			} catch (AddDeniedException e) {
				System.err
						.println("error writing to virtuoso, try it for each single statement");

				StmtIterator statements = modelToWrite.listStatements();

				while (statements.hasNext()) {
					try {
						Statement stmt = statements.next();
						System.err.println("next: "
								+ stmt.getSubject().getURI() + " "
								+ stmt.getPredicate().getURI() + " "
								+ stmt.getObject().toString());
						writeTo.add(stmt);
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

	private void registerModelsAtOntoWiki() {
		try {
			int requestID = RandomUtils.nextInt();
			String owBaseUri = config.getPropertyAsString(CONF_owBaseUri,
					baseNs);
			String owUser = config.getPropertyAsString(CONF_owUser, "Admin");
			String owPassword = config.getPropertyAsString(CONF_owPassword, "");
			URL ontoWikiUrl = new URL(owBaseUri + "jsonrpc/model/");

			JSONRPC2Session owSession = new JSONRPC2Session(ontoWikiUrl);

			String authString = owUser + ':' + owPassword;

			String authStringEnc = Base64.encodeBase64String(authString
					.getBytes());

			String authorizationValue = "Basic " + authStringEnc;
			owSession.addRequestProperty("Authorization", authorizationValue);

			String method = "create";
			Map<String, Object> params = new HashMap<String, Object>();

			for (String modelIri : models.keySet()) {
				params.put("modelIri", modelIri);
				JSONRPC2Request request = new JSONRPC2Request(method, requestID);
				request.setNamedParams(params);
				JSONRPC2Response response = owSession.send(request);

				// Print response result / error
				if (response.indicatesSuccess()) {
					System.out.println(response.getResult());
				} else {
					System.out.println(response.getError().getMessage());
				}
			}

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONRPC2SessionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}