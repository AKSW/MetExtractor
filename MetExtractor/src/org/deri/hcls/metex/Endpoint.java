package org.deri.hcls.metex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.VOIDX;
import org.deri.hcls.vocabulary.Vocabularies;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;
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
}
