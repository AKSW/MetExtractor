package org.aksw.metex.adapters;

import java.io.IOException;
import java.util.Collection;

import org.aksw.metex.ExtractorServiceAdapter;
import org.apache.jena.atlas.web.HttpException;
import org.deri.hcls.Endpoint;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.VOIDX;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * This method “manually” extracts meta information from the endpoint.
 * 
 * Some queries are taken from the void-impl project
 * (https://code.google.com/p/void-impl/wiki/SPARQLQueriesForStatistics)
 * 
 * Supports endpoint metadata (service level)
 * 
 * @author natanael
 * 
 */
public class ManualExtractorAdapter implements ExtractorServiceAdapter {

	private Collection<String> targetProperties;

	public ManualExtractorAdapter(Collection<String> properties) {
		targetProperties = properties;
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		Model model = ModelFactory.createDefaultModel();
		return getMetadata(new Endpoint(endpointUri, model));
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		Model model = ModelFactory.createDefaultModel();
		Resource endpointResource = model.createResource(endpoint.getUri());

		if (targetProperties == null) {
			// TODO somehow execute all queries
			// for now throw an Exception
			throw new IOException("no targetProperties specified");
		} else {
			System.err.println("Missing Properties are:");
			for (String prop : targetProperties) {
				System.err.println(prop);
			}
		}

		for (String property : targetProperties) {
			try {
				if (property.equals(VOID.triples.getURI())) {
					Literal countLiteral = getTripleCount(endpoint);
					if (countLiteral != null) {
						endpointResource
								.addProperty(VOID.triples, countLiteral);
					}

				} else if (property.equals(VOID.entities.getURI())) {
					Literal countLiteral = getEntitiyCount(endpoint);
					if (countLiteral != null) {
						endpointResource
								.addProperty(VOID.classes, countLiteral);
					}

				} else if (property.equals(VOID.classes.getURI())) {
					Literal countLiteral = getClassCount(endpoint);
					if (countLiteral != null) {
						endpointResource
								.addProperty(VOID.classes, countLiteral);
					}

				} else if (property.equals(VOID.properties.getURI())) {
					Literal countLiteral = getPropertyCount(endpoint);
					if (countLiteral != null) {
						endpointResource.addProperty(VOID.properties,
								countLiteral);
					}

				} else if (property.equals(VOID.distinctSubjects.getURI())) {
					Literal countLiteral = getSubjectCount(endpoint);
					if (countLiteral != null) {
						endpointResource.addProperty(VOID.distinctSubjects,
								countLiteral);
					}

				} else if (property.equals(VOID.distinctObjects.getURI())) {
					Literal countLiteral = getObjectCount(endpoint);
					if (countLiteral != null) {
						endpointResource.addProperty(VOID.distinctObjects,
								countLiteral);
					}

				} else if (property.equals(VOIDX.blankNodeCount.getURI())) {
					Literal countLiteral = getBnodeCount(endpoint);
					if (countLiteral != null) {
						endpointResource.addProperty(VOIDX.blankNodeCount,
								countLiteral);
					}

				} else if (property.equals(VOIDX.namespaceCount.getURI())) {
					/*
					 * TODO int namespaceCount = 0; Literal countLiteral = model
					 * .createTypedLiteral(namespaceCount);
					 * endpointResource.addProperty(VOIDX.namespaceCount,
					 * countLiteral);
					 */
				}
			} catch (QueryExecutionException e) {
				System.err.println("Manual extraction of value for " + property
						+ " failed.");
			}
		}

		return model;
	}

	private Literal getTripleCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "SELECT (COUNT(*) AS ?count) { ?s ?p ?o  }";
		return executeCountQuery(query, endpoint);
	}

	private Literal getEntitiyCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "SELECT (COUNT(distinct ?s) AS ?count) { ?s a []  }";
		return executeCountQuery(query, endpoint);
	}

	private Literal getClassCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "select (count( distinct ?class) as ?count) { [] a ?class } ";
		return executeCountQuery(query, endpoint);
	}

	private Literal getPropertyCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "SELECT (count(distinct ?p) as ?count) { ?s ?p ?o } ";
		return executeCountQuery(query, endpoint);
	}

	private Literal getSubjectCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "SELECT (count(distinct ?s) as ?count) { ?s ?p ?o } ";
		return executeCountQuery(query, endpoint);
	}

	private Literal getObjectCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "SELECT (count(distinct ?o) as ?count) { ?s ?p ?o } ";
		return executeCountQuery(query, endpoint);
	}

	private Literal getBnodeCount(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "";
		query += "select (count(distinct ?bn) as ?count) { ";
		query += " { ?bn ?p ?o . filter(isBlank(?bn)) } union ";
		query += " { ?s ?bn ?o . filter(isBlank(?bn)) } union ";
		query += " { ?s ?p ?bn . filter(isBlank(?bn)) } ";
		query += "}";

		return executeCountQuery(query, endpoint);
	}

	private Literal executeCountQuery(String query, Endpoint endpoint)
			throws QueryExecutionException {
		try {
			Model model = ModelFactory.createDefaultModel();
			ResultSet results = endpoint.execSelect(query);

			if (results.hasNext()) {
				QuerySolution result = results.next();

				try {
					long count = result.getLiteral("count").getLong();
					return model.createTypedLiteral(count);
				} catch (Exception e) {
					// totalClassCount was not a Literal
				}
			}
		} catch (HttpException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public String getServiceLink(String endpointUri) {
		// there is not service link
		return null;
	}

	@Override
	public String getServiceUri() {
		return "http://localhost/byqueries";
	}

	@Override
	public boolean isAvailable() {
		return true;
	}
}
