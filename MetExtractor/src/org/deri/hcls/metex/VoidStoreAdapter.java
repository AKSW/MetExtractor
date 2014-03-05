package org.deri.hcls.metex;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashSet;

import org.apache.jena.atlas.web.HttpException;
import org.deri.hcls.BlankNodeException;
import org.deri.hcls.Endpoint;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.VOIDX;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.vocabulary.RDF;

public class VoidStoreAdapter implements ExtractorServiceAdapter,
		EndpointListProviderAdapter {

	private static final String ENDPOINT_URI = "http://void.rkbexplorer.com/sparql/";

	private Endpoint voidStoreEndpoint;
	private Collection<String> datasetProperties;

	public VoidStoreAdapter(Collection<String> properties) {
		Model model = ModelFactory.createDefaultModel();
		voidStoreEndpoint = new Endpoint(ENDPOINT_URI, model);
		datasetProperties = properties;
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		return getMetadata(endpoint.getUri());
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		Model model = ModelFactory.createDefaultModel();
		try {
			Resource endpointDescription = voidStoreEndpoint.getResource(model
					.createResource(endpointUri));
			model.add(endpointDescription.listProperties());
			model.add(getDatasets(endpointUri, datasetProperties));
		} catch (HttpException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (BlankNodeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return model;
	}

	@Override
	public Collection<String> getAllEndpoints() throws IOException {
		return getSomeEndpoints(0);
	}

	@Override
	public Collection<String> getSomeEndpoints(int limit) throws IOException {

		String limitString = "";
		if (limit > 0) {
			limitString = "limit " + limit;
		}

		String queryDs = "";
		queryDs += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		queryDs += "prefix void: <http://rdfs.org/ns/void#> ";
		queryDs += "select distinct ?endpoint { ";
		queryDs += "	?ds void:sparqlEndpoint ?endpoint ";
		queryDs += "} " + limitString;
		// TODO remove limit

		String queryService = "";
		queryService += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		queryService += "prefix void: <http://rdfs.org/ns/void#> ";
		queryService += "select distinct ?endpoint { ";
		queryService += "	?endpoint a sd:Service ";
		queryService += "} ";

		Collection<String> endpoints = new HashSet<String>();
		try {
			ResultSet resultsDs = voidStoreEndpoint.execSelect(queryDs);

			while (resultsDs.hasNext()) {
				QuerySolution solution = resultsDs.next();
				String endpointUri = solution.get("endpoint").asResource()
						.getURI();
				endpoints.add(endpointUri);
			}

			ResultSet resultsService = voidStoreEndpoint
					.execSelect(queryService);

			while (resultsService.hasNext()) {
				QuerySolution solution = resultsService.next();
				String endpointUri = solution.get("endpoint").asResource()
						.getURI();
				endpoints.add(endpointUri);
			}
		} catch (QueryExceptionHTTP e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return endpoints;
	}

	private Model getDatasets(String endpointUri, Collection<String> properties) {

		Model model = ModelFactory.createDefaultModel();
		Resource endpointResource = model.createResource(endpointUri);

		String query = "";
		query += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		query += "prefix void: <http://rdfs.org/ns/void#> ";
		query += "select distinct ?ds ?p ?o { ";
		query += "	{ ";
		query += "		?ds void:sparqlEndpoint <" + endpointUri + "> ; ";
		query += "		    ?p ?o ";
		query += "	} ";
		query += "} ";

		try {
			ResultSet results = voidStoreEndpoint.execSelect(query);

			while (results.hasNext()) {
				QuerySolution solution = results.next();
				Resource ds = solution.get("ds").asResource();
				String predicateUri = solution.get("p").asResource().getURI();
				if (properties != null && properties.contains(predicateUri)) {
					if (org.deri.hcls.metex.Endpoint.termSubstitutions
							.containsKey(predicateUri)) {
						predicateUri = org.deri.hcls.metex.Endpoint.termSubstitutions
								.get(predicateUri);
					}
					Property p = model.createProperty(predicateUri);
					RDFNode o = solution.get("o");
					model.add(ds, p, o);
				}
			}
		} catch (QueryExceptionHTTP e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		ResIterator datasets = model.listResourcesWithProperty(
				VOID.sparqlEndpoint, endpointResource);
		while (datasets.hasNext()) {
			Resource dataset = datasets.next();
			dataset.addProperty(RDF.type, VOID.Dataset);
			endpointResource.addProperty(VOIDX.providesAccessTo, dataset);
		}

		return model;
	}

	@Override
	public String getServiceLink(String endpointUri) {
		try {
			return "http://void.rkbexplorer.com/browse/?uri="
					+ URLEncoder.encode(endpointUri, "utf-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public String getServiceUri() {
		return "http://void.rkbexplorer.com/";
	}

	@Override
	public boolean isAvailable() {
		try {
			return voidStoreEndpoint.isAvailable();
		} catch (Exception e) {
			return false;
		}
	}
}
