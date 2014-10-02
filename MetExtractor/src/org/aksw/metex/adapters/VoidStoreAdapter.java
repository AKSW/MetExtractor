package org.aksw.metex.adapters;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.aksw.metex.EndpointListProviderAdapter;
import org.aksw.metex.ExtractorServiceAdapter;
import org.apache.jena.atlas.web.HttpException;
import org.deri.hcls.BlankNodeException;
import org.deri.hcls.Endpoint;
import org.deri.hcls.LinkeddataHelper;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.VOIDX;
import org.deri.hcls.vocabulary.Vocabularies;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

public class VoidStoreAdapter implements ExtractorServiceAdapter,
		EndpointListProviderAdapter {

	private static final String ENDPOINT_URI = "http://void.rkbexplorer.com/sparql/";
	private static final String SCOVO_dimension = "http://purl.org/NET/scovo#dimension";

	private static Map<String, String> dimensionToProperty = new HashMap<String, String>();
	static {
		dimensionToProperty.put(VOID.NS + "numberOfResources",
				VOID.distinctSubjects.getURI());
		dimensionToProperty.put(VOID.NS + "numberOfTriples",
				VOID.triples.getURI());
		dimensionToProperty
				.put(VOID.NS + "numOfTriples", VOID.triples.getURI());
	}

	private static Map<String, String> mimeToFormat = new HashMap<String, String>();
	static {
		mimeToFormat.put("application/sparql-results+xml",
				Vocabularies.FORMATS_SPARQL_Results_XML.getURI());
		mimeToFormat.put("text/csv",
				Vocabularies.FORMATS_SPARQL_Results_CSV.getURI());
		mimeToFormat.put("application/sparql-results+json",
				Vocabularies.FORMATS_SPARQL_Results_JSON.getURI());
		mimeToFormat.put("text/turtle", Vocabularies.FORMATS_Turtle.getURI());
		mimeToFormat.put("text/rdf+n3", Vocabularies.FORMATS_N3.getURI());
		mimeToFormat.put("application/rdf+xml",
				Vocabularies.FORMATS_RDF_XML.getURI());
		mimeToFormat.put("application/n-triples",
				Vocabularies.FORMATS_NTriples.getURI());
	}

	private Endpoint voidStoreEndpoint;
	private Collection<String> datasetProperties;
	private Map<String, String> termSubstitutions;

	public VoidStoreAdapter(Collection<String> properties,
			Map<String, String> termSubstitutions) {
		Model model = ModelFactory.createDefaultModel();
		voidStoreEndpoint = new Endpoint(ENDPOINT_URI, model);
		datasetProperties = properties;
		this.termSubstitutions = termSubstitutions;
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
		query += "prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ";
		query += "select distinct ?ds ?p ?o ?io ?dim { ";
		query += "	{ ";
		query += "		?ds void:sparqlEndpoint <" + endpointUri + "> ; ";
		query += "		    ?p ?o ";
		query += "		optional { ";
		query += "			?o rdf:value ?io ; <" + SCOVO_dimension + "> ?dim ";
		query += "		} ";
		query += "	} ";
		query += "} ";

		try {
			ResultSet results = voidStoreEndpoint.execSelect(query);

			while (results.hasNext()) {
				QuerySolution solution = results.next();
				Resource ds = solution.get("ds").asResource();
				String predicateUri = solution.get("p").asResource().getURI();
				if (properties != null && properties.contains(predicateUri)) {
					if (termSubstitutions.containsKey(predicateUri)) {
						predicateUri = termSubstitutions.get(predicateUri);
					}
					Property p = model.createProperty(predicateUri);
					RDFNode o = solution.get("o");
					model.add(ds, p, o);
				} else if (predicateUri.equals(VOID.NS + "statItem")) {
					/*
					 * This is an endpoint specific property
					 */
					RDFNode dimensoion = solution.get("dim");
					if (dimensoion.isURIResource()) {
						predicateUri = dimensionToProperty.get(dimensoion
								.asResource().getURI());
						Property p = model.createProperty(predicateUri);
						RDFNode o = solution.get("io");
						model.add(ds, p, o);
					}
				} else if (predicateUri.equals(VOID.NS + "feature")) {
					RDFNode o = solution.get("o");
					if (!o.isURIResource()) {
						continue;
					}
					if (o.asResource().getLocalName().equals("sparql")) {
						Resource endpoint = model.createProperty(endpointUri);
						model.add(endpoint, Vocabularies.SD_defaultDataset, ds);
						try {
							Collection<RDFNode> values = LinkeddataHelper
									.getResourceValue(o.asResource(),
											DCTerms.format);

							for (RDFNode value : values) {
								if (value.isLiteral()) {
									String formatUri = mimeToFormat.get(value
											.asLiteral().getString());
									Resource format = model
											.createProperty(formatUri);

									model.add(endpoint,
											Vocabularies.SD_resultFormat,
											format);
								} else if (value.isURIResource()) {
									model.add(endpoint,
											Vocabularies.SD_resultFormat, value);
								}
							}
						} catch (Exception e) {
						}
					} else {
						String featureUri = o.asResource().getURI();
						if (o.asResource().getLocalName().equals("rdfxml")) {
							featureUri = "http://www.w3.org/ns/formats/RDF_XML";
						} else if (o.asResource().getLocalName()
								.equals("N-Triples")) {
							featureUri = "http://www.w3.org/ns/formats/N-Triples";
						} else if (o.asResource().getLocalName()
								.equals("Turtle_RDF")) {
							featureUri = "http://www.w3.org/ns/formats/Turtle";
						} else if (o.asResource().getLocalName()
								.equals("RDF_JSON")) {
							featureUri = "http://www.w3.org/ns/formats/RDF_JSON";
						}
						Resource feature = model.createProperty(featureUri);

						model.add(ds, VOID.feature, feature);
					}

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
