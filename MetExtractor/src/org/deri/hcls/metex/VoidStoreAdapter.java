package org.deri.hcls.metex;

import ie.deri.hcls.BlankNodeException;
import ie.deri.hcls.Endpoint;
import ie.deri.hcls.QueryExecutionException;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;

import org.apache.jena.atlas.web.HttpException;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;

public class VoidStoreAdapter implements WebServiceAdapter {

	private static final String voidStoreEndpointUri = "http://void.rkbexplorer.com/sparql/";

	private Endpoint voidStoreEndpoint;
	private Model model;

	public VoidStoreAdapter() {
		model = ModelFactory.createDefaultModel();
		voidStoreEndpoint = new Endpoint(voidStoreEndpointUri, model);
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
			model = getDatasets(endpointUri, model);
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
	public String getTitle(String endpoint) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<String> getAllEndpoints() throws IOException {

		String queryDs = "";
		queryDs += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		queryDs += "prefix void: <http://rdfs.org/ns/void#> ";
		queryDs += "select distinct ?endpoint { ";
		queryDs += "	?ds void:sparqlEndpoint ?endpoint ";
		queryDs += "} limit 10 ";
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

	private Model getDatasets(String endpointUri, Model model) {

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
				Property p = model.createProperty(solution.get("p")
						.asResource().getURI());
				RDFNode o = solution.get("o");
				model.add(ds, p, o);
			}
		} catch (QueryExceptionHTTP e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return model;
	}
}
