package org.deri.hcls.metex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.deri.hcls.Endpoint;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.ResourceHelper;
import org.deri.hcls.vocabulary.VOIDX;
import org.sindice.summary.simple.AbstractSimpleQuery.Structure;
import org.sindice.summary.simple.HTTPSimpleQuery;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;
import com.hp.hpl.jena.sparql.resultset.ResultSetException;
import com.hp.hpl.jena.vocabulary.RDFS;

public class SindiceAdapter implements ExtractorServiceAdapter {

	private Map<String, String> domainStructures = new HashMap<String, String>();

	private Map<String, String> predicateStructures = new HashMap<String, String>();

	private Model model;

	public SindiceAdapter(Model model) {
		this.model = model;
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		return getMetadata(endpoint.getUri());
	}

	@Override
	public Model getMetadata(String endpoint) throws IOException {
		try {
			getSindiceSummary(endpoint);
		} catch (Exception e) {
			throw new IOException(e);
		}
		return null;
	}

	public void getSindiceSummary(String endpointUri) throws Exception {

		Resource endpointResource = model.createResource(endpointUri);
		// Model model = ModelFactory.createDefaultModel();
		HTTPSimpleQuery hsq = new HTTPSimpleQuery(endpointUri);

		try {
			for (Structure st : hsq) {
				// do something here

				Resource domainResource = model.createResource(st.getDomain()
						.replaceAll("<([^>]*)>", "$1"));
				Resource predicateResource = model.createResource(st
						.getPredicate().replaceAll("<([^>]*)>", "$1"));
				Resource rangeResource;
				if (st.getRange() != null) {
					rangeResource = model.createResource(st.getRange()
							.replaceAll("<([^>]*)>", "$1"));
				} else {
					rangeResource = RDFS.Literal;
					continue;
				}

				String domainStructureUri = getDomainStructure(model,
						endpointUri, domainResource.getURI());
				String predicateStructureUri = null;
				Resource domainStructureResource;
				Resource predicateStructureResource;
				if (domainStructureUri == null) {
					domainStructureResource = ResourceHelper
							.createRandomResource(model);
					endpointResource.addProperty(VOIDX.hasStructure,
							domainStructureResource);
					domainStructureResource.addProperty(VOIDX.DOMAIN,
							domainResource);
				} else {
					domainStructureResource = model
							.createResource(domainStructureUri);
					predicateStructureUri = getPredicateStructure(model,
							domainStructureUri, predicateResource.getURI());
				}

				if (predicateStructureUri == null) {
					predicateStructureResource = ResourceHelper
							.createRandomResource(model);
					domainStructureResource.addProperty(VOIDX.hasPredicate,
							predicateStructureResource);
					predicateStructureResource.addProperty(VOIDX.PREDICATE,
							predicateResource);
				} else {
					predicateStructureResource = model
							.createResource(predicateStructureUri);
				}

				predicateStructureResource.addProperty(VOIDX.RANGE,
						rangeResource);
			}
		} finally {
			hsq.stopConnexion();

		}
	}

	private String getDomainStructure(Model model, String endpointUri,
			String domainUri) throws QueryExecutionException {
		if (domainStructures.containsKey(domainUri)) {
			return domainStructures.get(domainUri);
		} else {
			String query = "";
			query += "prefix voidx: <" + VOIDX.NS + "> ";
			query += "select ?structure { ";
			query += "	<" + endpointUri + "> voidx:hasStructure ?structure . ";
			query += "	?structure voidx:domain <" + domainUri + "> . ";
			query += "}";

			String domainStructureUri = execStructureQuery(model, query);
			if (domainStructureUri != null) {
				domainStructures.put(domainUri, domainStructureUri);
			}

			return domainStructureUri;
		}
	}

	private String getPredicateStructure(Model model,
			String domainStructureUri, String predicateUri)
			throws QueryExecutionException {
		if (predicateStructures.containsKey(predicateUri)) {
			return predicateStructures.get(predicateUri);
		} else {
			String query = "";
			query += "prefix voidx: <" + VOIDX.NS + "> ";
			query += "select ?structure { ";
			query += "	<" + domainStructureUri
					+ "> voidx:hasPredicate ?structure . ";
			query += "	?structure voidx:predicate <" + predicateUri + "> . ";
			query += "}";

			String predicateStructureUri = execStructureQuery(model, query);
			if (predicateStructureUri != null) {
				predicateStructures.put(predicateUri, predicateStructureUri);
			}

			return predicateStructureUri;
		}
	}

	private String execStructureQuery(Model model, String query)
			throws QueryExecutionException {
		try {
			Query qr = QueryFactory.create(query);
			QueryExecution x = QueryExecutionFactory.create(qr, model);

			ResultSet results = x.execSelect();

			if (results.hasNext()) {
				QuerySolution result = results.next();

				if (!result.getResource("structure").isURIResource()) {
					// TODO throw new exception
				}

				if (results.hasNext()) {
					System.err.println("Multiple structures for one domain");
				}

				return result.getResource("structure").getURI();
			} else {
				return null;
			}
		} catch (QueryParseException | ResultSetException | QueryExceptionHTTP e) {
			throw new QueryExecutionException(query, e);
		}
	}

	@Override
	public String getServiceLink(String endpointUri) {
		// there is no service link
		return null;
	}

	@Override
	public String getServiceUri() {
		return "https://github.com/sindice/sparqled/tree/master/sparql-summary";
	}

	@Override
	public boolean isAvailable() {
		return true;
	}

}
