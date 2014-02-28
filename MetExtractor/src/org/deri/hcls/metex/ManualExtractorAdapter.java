package org.deri.hcls.metex;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.jena.atlas.web.HttpException;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import ie.deri.hcls.Endpoint;
import ie.deri.hcls.QueryExecutionException;
import ie.deri.hcls.vocabulary.VOID;
import ie.deri.hcls.vocabulary.VOIDX;
import ie.deri.hcls.vocabulary.Vocabularies;

public class ManualExtractorAdapter implements WebServiceAdapter {

	private Model model;

	public ManualExtractorAdapter(Model model) {
		this.model = model;
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		return getMetadata(new Endpoint(endpointUri, model));
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		Resource endpointResource = model.createResource(endpoint.getUri());
		Collection<String> missingProperties = getListOfMissingProperties(
				endpointResource, null);

		for (String property : missingProperties) {
			try {
				if (property.equals(VOID.triples.getURI())) {

				} else if (property.equals(VOID.entities.getURI())) {

				} else if (property.equals(VOID.classes.getURI())) {
					int classCount = getClassCount(endpoint);
					Literal countLiteral = model.createTypedLiteral(classCount);
					endpointResource.addProperty(VOID.classes, countLiteral);
				} else if (property.equals(VOID.properties.getURI())) {

				} else if (property.equals(VOID.distinctSubjects.getURI())) {

				} else if (property.equals(VOID.distinctObjects.getURI())) {

				} else if (property.equals(VOIDX.blankNodeCount.getURI())) {
					int bnodeCount = getBnodeCount(endpoint);
					Literal countLiteral = model.createTypedLiteral(bnodeCount);
					endpointResource.addProperty(VOIDX.blankNodeCount,
							countLiteral);
				} else if (property.equals(Vocabularies.sd_endpoint.getURI())) {
					endpointResource.addProperty(Vocabularies.sd_endpoint,
							endpointResource);
				}
			} catch (QueryExecutionException e) {
				e.printStackTrace();
			}
		}

		return null;
	}

	@Override
	public String getTitle(String endpoint) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<String> getAllEndpoints() throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	private Collection<String> getListOfMissingProperties(Resource resource,
			Collection<String> targetProperties) {
		Model model = ModelFactory.createDefaultModel();
		Set<String> missingProperties = new HashSet<String>();
		for (String predicateUri : targetProperties) {
			Property property = model.createProperty(predicateUri);
			if (!resource.hasProperty(property)) {
				missingProperties.add(predicateUri);
			}
		}
		return missingProperties;
	}

	private int getClassCount(Endpoint endpoint) throws QueryExecutionException {

		String query = "";
		query += "select  (count( distinct ?class) as ?totalClassCount) { ";
		query += "	[] a ?class ";
		query += "} ";

		try {
			ResultSet results = endpoint.execSelect(query);

			if (results.hasNext()) {
				QuerySolution result = results.next();

				try {
					return result.getLiteral("totalClassCount").getInt();
				} catch (Exception e) {
					// totalClassCount was not a Literal
				}
			}
		} catch (HttpException e) {
			e.printStackTrace();
		}

		return -1;
	}

	private int getBnodeCount(Endpoint endpoint) {
		String query = "select count(?bn) {"
				+ "{ ?bn ?p ?o . filter(isBlank(?bn))} union {?s ?bn ?o . filter(isBlank(?bn)) } union {?s ?p ?bn . filter(isBlank(?bn))}"
				+ " }	LIMIT 1000";
		return 0;
	}
}
