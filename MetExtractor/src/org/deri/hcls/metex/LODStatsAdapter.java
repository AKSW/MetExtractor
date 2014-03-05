package org.deri.hcls.metex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.deri.hcls.Endpoint;
import org.deri.hcls.QueryExecutionException;
import org.deri.hcls.vocabulary.VOID;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;

public class LODStatsAdapter implements ExtractorServiceAdapter {

	private static String ENDPOINT_URI = "http://stats.lod2.eu/sparql/";
	private static String CRITERIA_NS = "http://stats.lod2.eu/rdf/qb/criteria/";

	private static String templateQuery = "";
	static {
		templateQuery += "prefix qb: <http://stats.lod2.eu/rdf/qb/> ";
		templateQuery += "select distinct  ?value ?criterion { ";
		templateQuery += "  ?sds qb:sourceDataset <%s> ; qb:value ?value ; qb:statisticalCriterion ?criterion ";
		templateQuery += "}";
	}

	private static Map<String, Property> criteriaToVoid = new HashMap<String, Property>();
	static {
		criteriaToVoid.put(CRITERIA_NS + "triples", VOID.triples);
		criteriaToVoid.put(CRITERIA_NS + "entitiesMentioned", VOID.entities);
		criteriaToVoid.put(CRITERIA_NS + "literals", null);
		criteriaToVoid.put(CRITERIA_NS + "links", null);
		criteriaToVoid.put(CRITERIA_NS + "blanksAsObject", null);
		criteriaToVoid.put(CRITERIA_NS + "blanksAsSubject", null);
		criteriaToVoid.put(CRITERIA_NS + "typedSubjects", null);
		criteriaToVoid.put(CRITERIA_NS + "averageTypedStringLength", null);
		criteriaToVoid.put(CRITERIA_NS + "subclassUsage", null);
		criteriaToVoid.put(CRITERIA_NS + "averageUntypedStringLength", null);
	}

	private Endpoint lodStatsEndpoint;
	private Model model;

	public LODStatsAdapter() {
		model = ModelFactory.createDefaultModel();
		lodStatsEndpoint = new Endpoint(ENDPOINT_URI, model);
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		Model model = ModelFactory.createDefaultModel();
		Resource endpointResource = model.createResource(endpointUri);
		String query = String.format(templateQuery, endpointUri);
		try {
			ResultSet results = lodStatsEndpoint.execSelect(query);

			while (results.hasNext()) {
				QuerySolution solution = results.next();
				Literal value = solution.get("value").asLiteral();
				String criterion = solution.get("criterion").asResource()
						.getURI();
				Property property = criteriaToVoid.get(criterion);
				if (property != null && value.getInt() > 0) {
					model.add(endpointResource, property, value);
				}
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

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		return getMetadata(endpoint.getUri());
	}

	@Override
	public String getServiceLink(String endpointUri) {
		return "http://stats.lod2.eu/datasets/" + endpointUri;
	}

	@Override
	public String getServiceUri() {
		return "http://stats.lod2.eu/";
	}

	@Override
	public boolean isAvailable() {
		try {
			return lodStatsEndpoint.isAvailable();
		} catch (Exception e) {
			return false;
		}
	}

}
