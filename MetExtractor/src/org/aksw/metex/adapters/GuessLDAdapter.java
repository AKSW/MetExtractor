package org.aksw.metex.adapters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.aksw.metex.ExtractorServiceAdapter;
import org.apache.jena.atlas.web.HttpException;
import org.deri.hcls.BlankNodeException;
import org.deri.hcls.Endpoint;
import org.deri.hcls.QueryExecutionException;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class GuessLDAdapter implements ExtractorServiceAdapter {

	private Collection<String> endpointFetchProperties;
	private Map<String, String> termSubstitutions;

	public GuessLDAdapter(Collection<String> endpointFetchProperties, Map<String, String> termSubstitutions) {
		this.endpointFetchProperties = endpointFetchProperties;
		this.termSubstitutions = termSubstitutions;
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		Model model = ModelFactory.createDefaultModel();
		Resource endpointResource = model.createResource(endpointUri.trim());
		Endpoint endpoint = new Endpoint(endpointResource);
		return getMetadata(endpoint);
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {

		/*
		 * Get further describing instances. E.g. some endpoints use
		 * http://localhost:8890/sparql a sd:Service
		 */

		Model model = ModelFactory.createDefaultModel();
		Resource indirectResource = null;
		try {
			indirectResource = getIndirectMetadata(endpoint);
		} catch (QueryExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (indirectResource != null) {
			StmtIterator properties = indirectResource.listProperties();
			model.add(properties);
		}

		return null;
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

	private Resource getIndirectMetadata(Endpoint endpoint)
			throws QueryExecutionException {
		Set<Resource> describingInstances = getDescribingInstancesCandidates(endpoint);

		Model model = ModelFactory.createDefaultModel();
		Resource indirectDescription = model.createResource(endpoint.getUri());

		for (Resource instance : describingInstances) {
			try {
				Resource instanceResource = endpoint.getResource(instance,
						endpointFetchProperties, termSubstitutions);
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

	private Set<Resource> getDescribingInstancesCandidates(Endpoint endpoint)
			throws QueryExecutionException {

		ArrayList<String> keywords = new ArrayList<String>();
		// this should also match
		// http://www.w3.org/ns/sparql-service-description#Service
		keywords.add("Service");
		keywords.add("Endpoint");

		Model model = ModelFactory.createDefaultModel();

		List<Resource> allClasses = getClasses(endpoint);
		Set<Resource> instanceCandidates = new HashSet<Resource>();
		for (Resource classHit : allClasses) {
			for (String keyword : keywords) {
				if (classHit.getLocalName().contains(keyword)) {
					try {
						Set<Resource> instances = endpoint.getInstances(
								classHit, model, 0);
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

	private List<Resource> getClasses(Endpoint endpoint)
			throws QueryExecutionException {
		String query = "";
		query += "select distinct ?class { ";
		query += "	[] a ?class ";
		query += "} ";

		ArrayList<Resource> classes = new ArrayList<Resource>();

		try {
			ResultSet results = endpoint.execSelect(query);

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
					+ endpoint.getUri());
		}

		return classes;
	}

}
