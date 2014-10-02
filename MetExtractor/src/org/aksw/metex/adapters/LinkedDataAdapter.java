package org.aksw.metex.adapters;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.aksw.metex.ExtractorServiceAdapter;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.riot.RiotException;
import org.deri.hcls.BlankNodeException;
import org.deri.hcls.Endpoint;
import org.deri.hcls.LinkeddataHelper;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.ResourceHelper;
import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.VOIDX;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.sparql.resultset.ResultSetException;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class LinkedDataAdapter implements ExtractorServiceAdapter {

	private Collection<String> endpointFetchProperties;
	private Collection<String> datasetFetchProperties;
	private Map<String, String> termSubstitutions;

	public LinkedDataAdapter(Collection<String> endpointFetchProperties,
			Collection<String> datasetFetchProperties,
			Map<String, String> termSubstitutions) {
		this.endpointFetchProperties = endpointFetchProperties;
		this.datasetFetchProperties = datasetFetchProperties;
		this.termSubstitutions = termSubstitutions;
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		Model model = ModelFactory.createDefaultModel();
		Resource endpointResource = model.createResource(endpointUri);
		Endpoint endpoint = new Endpoint(endpointResource);
		return getMetadata(endpoint);
	}

	/**
	 * get metadata from endpoint for describing the endpoint and its datasets
	 */
	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		Model endpointMetadata = getLinkedData(endpoint);

		return endpointMetadata;
	}

	@Override
	public String getServiceLink(String endpointUri) {
		return null;
	}

	@Override
	public String getServiceUri() {
		return null;
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

	private Model getLinkedData(Endpoint endpoint) {
		Resource linkedDataResource = null;
		Resource sparqlResource = null;

		Model model = ModelFactory.createDefaultModel();
		String endpointUri = endpoint.getUri();
		Resource endpointResource = model.createResource(endpointUri);

		try {
			linkedDataResource = LinkeddataHelper.getResource(
					endpointResource.getURI(), endpointFetchProperties,
					termSubstitutions);

		} catch (SocketTimeoutException e) {
			System.err.println("endpoint: " + endpointUri
					+ " is too slow, socket timed out");
			endpointResource
					.addLiteral(RDFS.comment,
							"This endpoint is too slow and the HTTP connection timed out after 10 seconds");
		} catch (RiotException e) {
			System.err.println("endpoint: " + endpointUri
					+ " returned an invalide RDF description");
			endpointResource.addLiteral(
					RDFS.comment,
					"The endpoint returned invalide RDF. Exception: "
							+ e.getMessage());
		} catch (QueryExceptionHTTP e) {
			System.err.println("endpoint: " + endpointUri
					+ " not available, response was: " + e.getResponseCode()
					+ " with message: " + e.getResponseMessage());
			endpointResource.addLiteral(VOIDX.httpReturnCode,
					e.getResponseCode());
			String message = e.getMessage();
			if (message != null) {
				endpointResource.addLiteral(RDFS.comment,
						"The endpoint returned and HTTP error code and following message: "
								+ message);
			}
		} catch (IOException e) {
			endpointResource
					.addLiteral(
							RDFS.comment,
							"Retreiving data from the endpoint caused an IOException with following message: "
									+ e.getMessage());
		} catch (Exception e) {
			System.err.println("Caut an exception of type: "
					+ e.getClass().getName());
			e.printStackTrace();
		}

		try {
			sparqlResource = endpoint.getResource(endpointResource,
					endpointFetchProperties, termSubstitutions);
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (BlankNodeException e) {
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		if (linkedDataResource != null) {
			StmtIterator properties = linkedDataResource.listProperties();
			model.add(properties);
		}

		if (sparqlResource != null) {
			StmtIterator properties = sparqlResource.listProperties();
			model.add(properties);
		}

		model.add(getDatasetMetadata(endpoint));

		return model;
	}

	private Model getDatasetMetadata(Endpoint endpoint) {

		Model model = ModelFactory.createDefaultModel();
		String endpointUri = endpoint.getUri();
		Resource endpointResource = model.createResource(endpointUri);

		try {
			Collection<Resource> datasets = getDatasetDescriptions(endpoint);

			System.err.println("Found " + datasets.size() + " datasets");

			Literal countLiteral;
			int datasetCount = datasets.size();
			if (datasetCount > 10) {
				// TODO get number of datasets
				String datasetCountNote = datasetCount + "+";
				countLiteral = model.createTypedLiteral(datasetCountNote);
			} else {
				countLiteral = model.createTypedLiteral(datasetCount);
			}

			endpointResource.addProperty(VOIDX.datasetCount, countLiteral);

			for (Resource dataset : datasets) {
				model.add(endpointResource, VOIDX.providesAccessTo, dataset);
				StmtIterator properties = dataset.listProperties();
				model.add(properties);
			}
		} catch (QueryExecutionException e) {
			System.err.println("Couldn't get any datasets for: "
					+ endpointResource.getURI());
			e.printStackTrace();
		}

		/*
		 * TODO get classes, and all the other data
		 */

		return model;
	}

	private Collection<Resource> getDatasetDescriptions(Endpoint endpoint)
			throws QueryExecutionException {

		String queryVoidDataset = "";
		queryVoidDataset += "prefix void: <" + VOID.NS + "> ";
		queryVoidDataset += "prefix dct: <" + DCTerms.NS + "> ";
		queryVoidDataset += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		queryVoidDataset += "select ?dataset ?predicate ?value { ";
		queryVoidDataset += "	?dataset a void:Dataset ; ";
		queryVoidDataset += "			 ?predicate ?value . ";
		queryVoidDataset += "} limit 11";

		String queryDctDataset = "";
		queryDctDataset += "prefix void: <" + VOID.NS + "> ";
		queryDctDataset += "prefix dct: <" + DCTerms.NS + "> ";
		queryDctDataset += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		queryDctDataset += "select ?dataset ?predicate ?value { ";
		queryDctDataset += "	?dataset a dct:Dataset ; ";
		queryDctDataset += "			 ?predicate ?value . ";
		queryDctDataset += "} limit 11";

		String queryByPredicate = "";
		queryByPredicate += "prefix void: <" + VOID.NS + "> ";
		queryByPredicate += "prefix dct: <" + DCTerms.NS + "> ";
		queryByPredicate += "prefix sd: <http://www.w3.org/ns/sparql-service-description#> ";
		queryByPredicate += "select ?dataset ?predicate ?value { ";
		queryByPredicate += "	?dataset void:endpoint <" + endpoint.getUri()
				+ "> ; ";
		queryByPredicate += "			 ?predicate ?value . ";
		queryByPredicate += "} limit 11";

		Model model = ModelFactory.createDefaultModel();
		Set<Resource> datasets = new HashSet<Resource>();

		try {
			datasets.addAll(readDatasetMetadataFromResultSet(endpoint,
					queryVoidDataset, model, datasetFetchProperties));
		} catch (QueryExceptionHTTP e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		try {
			datasets.addAll(readDatasetMetadataFromResultSet(endpoint,
					queryDctDataset, model, datasetFetchProperties));
		} catch (QueryExceptionHTTP e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		try {
			datasets.addAll(readDatasetMetadataFromResultSet(endpoint,
					queryByPredicate, model, datasetFetchProperties));
		} catch (QueryExceptionHTTP e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		return datasets;
	}

	private Collection<Resource> readDatasetMetadataFromResultSet(
			Endpoint endpoint, String query, Model model,
			Collection<String> properties) throws QueryExceptionHTTP,
			QueryExecutionException {
		ResultSet results = endpoint.execSelect(query);

		ArrayList<Resource> datasets = new ArrayList<Resource>();

		while (results.hasNext()) {
			QuerySolution result = results.next();
			Resource dataset = result.getResource("dataset");
			Resource predicate = result.getResource("predicate");
			RDFNode value = result.get("value");

			/*
			 * check if predicate is contained in datasetProperties
			 */
			if (predicate == null || !predicate.isURIResource()
					|| !properties.contains(predicate.getURI())) {
				continue;
			}

			if (dataset == null) {
				System.err.println("Dataset was null but p: " + predicate
						+ ", v: " + value + " for endpoint: "
						+ endpoint.getUri() + " ignorring this");
				continue;
			}

			int index;
			if ((index = datasets.indexOf(dataset)) > 0) {
				dataset = datasets.get(index);
			} else {
				if (dataset.isURIResource()) {
					dataset = model.createResource(dataset.getURI());
				} else {
					dataset = ResourceHelper.createRandomResource(model);
				}
				dataset.addProperty(RDF.type, VOID.Dataset);
				dataset.addProperty(VOID.sparqlEndpoint,
						endpoint.getEndpointResource());
				datasets.add(dataset);
			}

			/*
			 * rewrite termSubstitution
			 */
			String predicateUri = predicate.getURI();
			if (termSubstitutions.containsKey(predicateUri)) {
				predicateUri = termSubstitutions.get(predicateUri);
			}

			Property property = model.createProperty(predicateUri);
			dataset.addProperty(property, value);
		}

		return datasets;
	}
}
