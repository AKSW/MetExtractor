package org.deri.hcls.metex;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.riot.RiotException;
import org.deri.hcls.BlankNodeException;
import org.deri.hcls.LinkeddataHelper;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.ResourceHelper;
import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.VOIDX;
import org.deri.hcls.vocabulary.Vocabularies;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.sparql.resultset.ResultSetException;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class Endpoint extends org.deri.hcls.Endpoint {

	/**
	 * The properties, we use in out data model to describe datasets
	 */
	public static ArrayList<String> datasetProperties = new ArrayList<String>();

	/**
	 * The properties, we fetch, but which will be mapped to datasetProperties
	 */
	public static ArrayList<String> datasetFetchProperties = new ArrayList<String>();

	static {
		datasetProperties.add(RDFS.label.getURI());
		datasetProperties.add(DC.title.getURI());
		datasetProperties.add(DC.description.getURI());
		datasetProperties.add(FOAF.homepage.getURI());
		datasetProperties.add(DCTerms.created.getURI());
		datasetProperties.add(DCTerms.creator.getURI());
		datasetProperties.add(DCTerms.publisher.getURI());
		datasetProperties.add(DCTerms.license.getURI());
		datasetProperties.add(DCTerms.rights.getURI());
		datasetProperties.add(DCTerms.source.getURI());
		datasetProperties.add(DCTerms.description.getURI());
		datasetProperties.add("http://www.w3.org/ns/prov#wasDerivedFrom");
		datasetProperties.add("http://purl.org/pav/retrievedOn");
		datasetProperties.add("http://www.w3.org/ns/dcat#distribution");
		datasetProperties.add(VOID.dataDump.getURI());
		datasetProperties.add(VOID.triples.getURI());
		datasetProperties.add(VOID.entities.getURI());
		datasetProperties.add(VOID.classes.getURI());
		datasetProperties.add(VOID.properties.getURI());
		datasetProperties.add(VOID.distinctSubjects.getURI());
		datasetProperties.add(VOID.distinctObjects.getURI());
		datasetProperties.add(VOID.exampleResource.getURI());
		datasetProperties.add(VOID.uriRegexPattern.getURI());
		datasetProperties.add(VOID.NS + "rootResource");
		datasetProperties.add(DCTerms.subject.getURI());
		datasetFetchProperties.add("http://bio2rdf.org/dcat:distribution");
		datasetFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_triple_count");
		datasetFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_entity_count");
		datasetFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_class_count");
		datasetFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_unique_predicate_count");
		datasetFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_unique_subject_count");
		datasetFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_unique_object_count");
		datasetFetchProperties.add(DC.subject.getURI());

		datasetFetchProperties.addAll(datasetProperties);
	}

	/**
	 * The properties, we use in out data model to describe endpoints
	 */
	public static ArrayList<String> endpointProperties = new ArrayList<String>();

	/**
	 * The properties, we fetch, but which will be mapped to endpointProperties
	 */
	public static ArrayList<String> endpointFetchProperties = new ArrayList<String>();

	static {
		endpointProperties.add("http://www.w3.org/2000/01/rdf-schema#label");
		endpointProperties
				.add("http://www.w3.org/ns/sparql-service-description#feature");
		endpointProperties
				.add("http://www.w3.org/ns/sparql-service-description#resultFormat");
		endpointProperties
				.add("http://www.w3.org/ns/sparql-service-description#supportedLanguage");
		endpointProperties.add(Vocabularies.SD_endpoint.getURI());
		endpointProperties.add(VOID.triples.getURI());
		endpointProperties.add(VOID.entities.getURI());
		endpointProperties.add(VOID.classes.getURI());
		endpointProperties.add(VOID.properties.getURI());
		endpointProperties.add(VOID.distinctSubjects.getURI());
		endpointProperties.add(VOID.distinctObjects.getURI());
		endpointProperties.add(VOIDX.blankNodeCount.getURI());
		endpointProperties.add(VOIDX.namespaceCount.getURI());
		endpointProperties.add(VOIDX.datasetCount.getURI());
		endpointProperties.add(DCTerms.subject.getURI());

		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_url");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_triple_count");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_entity_count");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_class_count");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_unique_predicate_count");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_unique_subject_count");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_unique_object_count");
		endpointFetchProperties
				.add("http://bio2rdf.org/dataset_vocabulary:has_type_count");
		endpointFetchProperties.add(DC.subject.getURI());

		endpointFetchProperties.addAll(endpointProperties);
	}

	public static Map<String, String> termSubstitutions = new HashMap<String, String>();
	static {
		termSubstitutions.put(
				"http://www.w3.org/ns/sparql-service-description#url",
				Vocabularies.SD_endpoint.getURI());
		termSubstitutions.put("http://bio2rdf.org/dataset_vocabulary:has_url",
				Vocabularies.SD_endpoint.getURI());
		termSubstitutions.put(
				"http://bio2rdf.org/dataset_vocabulary:has_triple_count",
				VOID.triples.getURI());
		termSubstitutions.put(
				"http://bio2rdf.org/dataset_vocabulary:has_class_count",
				VOID.classes.getURI());
		termSubstitutions.put(
				"http://bio2rdf.org/dataset_vocabulary:has_entity_count",
				VOID.entities.getURI());
		termSubstitutions.put(
				"http://bio2rdf.org/dataset_vocabulary:has_type_count",
				VOID.classes.getURI());
		termSubstitutions
				.put("http://bio2rdf.org/dataset_vocabulary:has_unique_predicate_count",
						VOID.properties.getURI());
		termSubstitutions
				.put("http://bio2rdf.org/dataset_vocabulary:has_unique_subject_count",
						VOID.distinctSubjects.getURI());
		termSubstitutions
				.put("http://bio2rdf.org/dataset_vocabulary:has_unique_object_count",
						VOID.distinctObjects.getURI());
		termSubstitutions.put("http://bio2rdf.org/dcat:distribution",
				"http://www.w3.org/ns/dcat#distribution");
		termSubstitutions.put(DC.subject.getURI(), DCTerms.subject.getURI());
	}

	/**
	 * Constructor for the endpoint
	 * 
	 * @param endpointUri
	 *            the URI of the endpoint
	 */
	public Endpoint(Resource endpointResource) {
		super(endpointResource);
	}

	/**
	 * Constructor for the endpoint
	 * 
	 * @param endpointUri
	 *            the URI of the endpoint
	 */
	public Endpoint(String endpointUri, Model model) {
		super(endpointUri, model);
	}

	public Model getMetadata() {
		Resource linkedDataResource = null;
		Resource sparqlResource = null;
		Resource indirectResource = null;

		try {
			linkedDataResource = LinkeddataHelper.getResource(endpointResource);

		} catch (SocketTimeoutException e) {
			System.err.println("endpoint: " + getUri()
					+ " is to slow, socket timed out");
			endpointResource
					.addLiteral(RDFS.comment,
							"This endpoint is to slow and the HTTP connection timed out after 10 seconds");
		} catch (RiotException e) {
			System.err.println("endpoint: " + getUri()
					+ " returned an invalide RDF description");
			endpointResource.addLiteral(
					RDFS.comment,
					"The endpoint returned invalide RDF. Exception: "
							+ e.getMessage());
		} catch (QueryExceptionHTTP e) {
			System.err.println("endpoint: " + getUri()
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
			sparqlResource = getResource(endpointResource,
					endpointFetchProperties);
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (BlankNodeException e) {
			e.printStackTrace();
		} catch (QueryExecutionException e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		/*
		 * Get further describing instances. E.g. some endpoints use
		 * http://localhost:8890/sparql a sd:Service
		 */
		try {
			indirectResource = getIndirectMetadata();
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Model model = ModelFactory.createDefaultModel();

		if (linkedDataResource != null) {
			StmtIterator properties = linkedDataResource.listProperties();
			model.add(properties);
		}

		if (sparqlResource != null) {
			StmtIterator properties = sparqlResource.listProperties();
			model.add(properties);
		}

		if (indirectResource != null) {
			StmtIterator properties = indirectResource.listProperties();
			model.add(properties);
		}

		model.add(getDatasetMetadata());

		return model;
	}

	public Resource getResource(Resource resource,
			ArrayList<String> predicateFilter) throws HttpException,
			BlankNodeException, QueryExecutionException {
		Resource resourceOutput = super.getResource(resource, predicateFilter);
		Iterator<String> iterator = termSubstitutions.keySet().iterator();
		Model model = ModelFactory.createDefaultModel();
		while (iterator.hasNext()) {
			String propertyFromUri = iterator.next();
			Property propertyFrom = model.createProperty(propertyFromUri);
			StmtIterator statementsIt = resourceOutput
					.listProperties(propertyFrom);
			List<Statement> statements = statementsIt.toList();
			if (!statements.isEmpty()) {
				String propertyToUri = termSubstitutions.get(propertyFromUri);
				Property propertyTo = model.createProperty(propertyToUri);
				for (Statement statement : statements) {
					resourceOutput.addProperty(propertyTo,
							statement.getObject());
				}
				resourceOutput.removeAll(propertyFrom);
			}
		}
		return resourceOutput;
	}

	public Model getDatasetMetadata() {

		Model model = ModelFactory.createDefaultModel();
		try {
			Collection<Resource> datasets = getDatasetDescriptions();

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
			System.err.println("Couldn't get any datasets for: " + getUri());
			e.printStackTrace();
		}

		/**
		 * TODO get classes, and all the other data
		 */

		return model;
	}

	private Collection<Resource> getDatasetDescriptions()
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
		queryByPredicate += "	?dataset void:endpoint <" + getUri() + "> ; ";
		queryByPredicate += "			 ?predicate ?value . ";
		queryByPredicate += "} limit 11";

		Model model = ModelFactory.createDefaultModel();
		Set<Resource> datasets = new HashSet<Resource>();

		try {
			datasets.addAll(readDatasetMetadataFromResultSet(queryVoidDataset,
					model, datasetFetchProperties));
		} catch (QueryExceptionHTTP e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		try {
			datasets.addAll(readDatasetMetadataFromResultSet(queryDctDataset,
					model, datasetFetchProperties));
		} catch (QueryExceptionHTTP e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		try {
			datasets.addAll(readDatasetMetadataFromResultSet(queryByPredicate,
					model, datasetFetchProperties));
		} catch (QueryExceptionHTTP e) {
			e.printStackTrace();
		} catch (ResultSetException e) {
			e.printStackTrace();
		}

		return datasets;
	}

	private Collection<Resource> readDatasetMetadataFromResultSet(String query,
			Model model, Collection<String> properties)
			throws QueryExceptionHTTP, QueryExecutionException {
		ResultSet results = this.execSelect(query);

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
						+ ", v: " + value + " for endpoint: " + getUri()
						+ " ignorring this");
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
				dataset.addProperty(VOID.sparqlEndpoint, endpointResource);
				datasets.add(dataset);
			}

			Property property = model.createProperty(predicate.getURI());
			dataset.addProperty(property, value);
		}

		return datasets;
	}

	private Resource getIndirectMetadata() throws QueryExecutionException {
		Set<Resource> describingInstances = getDescribingInstancesCandidates();

		Model model = ModelFactory.createDefaultModel();
		Resource indirectDescription = model.createResource(getUri());

		for (Resource instance : describingInstances) {
			try {
				Resource instanceResource = getResource(instance,
						endpointProperties);
				StmtIterator iterator = instanceResource.listProperties();
				while (iterator.hasNext()) {
					Statement statement = iterator.next();
					indirectDescription.addProperty(statement.getPredicate(),
							statement.getObject());
				}
			} catch (HttpException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (BlankNodeException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return indirectDescription;
	}

	private Set<Resource> getDescribingInstancesCandidates()
			throws QueryExecutionException {

		ArrayList<String> keywords = new ArrayList<String>();
		// this should also match
		// http://www.w3.org/ns/sparql-service-description#Service
		keywords.add("Service");
		keywords.add("Endpoint");

		Model model = ModelFactory.createDefaultModel();

		List<Resource> allClasses = getClasses();
		Set<Resource> instanceCandidates = new HashSet<Resource>();
		for (Resource classHit : allClasses) {
			for (String keyword : keywords) {
				if (classHit.getLocalName().contains(keyword)) {
					try {
						Set<Resource> instances = getInstances(classHit, model,
								0);
						instanceCandidates.addAll(instances);
					} catch (HttpException e) {
						System.err
								.println("Caucht exception, when fetching instances of class: "
										+ classHit);
						e.printStackTrace();
					} catch (QueryExecutionException e) {
						System.err
								.println("Caucht exception, when fetching instances of class: "
										+ classHit);
						e.printStackTrace();
					}
				}
			}
		}

		return instanceCandidates;
	}

	private List<Resource> getClasses() throws QueryExecutionException {
		String query = "";
		query += "select distinct ?class { ";
		query += "	[] a ?class ";
		query += "} ";

		ArrayList<Resource> classes = new ArrayList<Resource>();

		try {
			ResultSet results = this.execSelect(query);

			while (results.hasNext()) {
				QuerySolution result = results.next();

				try {
					Resource classResource = result.getResource("class");
					if (classResource.isURIResource()) {
						classes.add(classResource);
					}
				} catch (Exception e) {
					// The class was not a Resource
				}
			}
		} catch (HttpException e) {
			System.err.println("Could not get classes for endpoint: "
					+ getUri());
		}

		return classes;
	}
}
