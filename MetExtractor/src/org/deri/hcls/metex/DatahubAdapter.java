package org.deri.hcls.metex;

import ie.deri.hcls.vocabulary.Vocabularies;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;

import ie.deri.hcls.Endpoint;

public class DatahubAdapter implements WebServiceAdapter {

	private static final String API_BASE = "http://datahub.io/api/2/";
	private static final String ENDPOINT_LIST_URL = API_BASE
			+ "search/resource?format=api/sparql&all_fields=1&limit=1000";
	private static final String DATASET_BASE = API_BASE + "rest/dataset/";

	private static Map<String, String> endpointIds = new HashMap<String, String>();

	public DatahubAdapter() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		return getMetadata(endpoint.getUri());
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		String endpointId = endpointIds.get(endpointUri);
		URL url = new URL(DATASET_BASE + endpointId);
		URLConnection conn = url.openConnection();
		InputStream inputstream = conn.getInputStream();

		JsonObject metadataJson = JSON.parse(inputstream);
		JsonValue resources = metadataJson.get("resources");

		Model model = ModelFactory.createDefaultModel();

		if (resources.isArray()) {
			for (JsonValue resource : resources.getAsArray()) {
				if (resource.isObject()) {
					String format = resource.getAsObject().get("format")
							.getAsString().value();
					if (format.equals("api/sparql")) {
						/*
						 * TODO see what metadata we can use
						 */
					} else if (format.equals("meta/void")) {
						Resource datasetResource = readDatasetMetaFromJson(metadataJson,
								resource.getAsObject(), model);
						Resource endpointResource = model.createResource(endpointUri);
						endpointResource.addProperty(Vocabularies.sd_defaultDataset, datasetResource);
					}
				}
			}
		}

		/*
		 * TODO see how we can use the dataset level metadat
		 */

		return null;
	}

	@Override
	public String getTitle(String endpoint) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<String> getAllEndpoints() throws IOException {
		return getEndpoints(10);
	}

	public Collection<String> getEndpoints(int max) throws IOException {
		URL url = new URL(ENDPOINT_LIST_URL);
		URLConnection conn = url.openConnection();
		InputStream inputstream = conn.getInputStream();

		JsonObject endpointsJson = JSON.parse(inputstream);
		JsonValue countValue = endpointsJson.get("count");
		if (countValue.isNumber()) {
			int count = countValue.getAsNumber().value().intValue();
			System.err.println(count
					+ " endpoints are contained in the given file");
		}
		JsonValue results = endpointsJson.get("results");

		if (results.isArray()) {
			Iterator<JsonValue> inputIterator = results.getAsArray().iterator();

			int i = 0;
			while (inputIterator.hasNext()) {
				JsonValue result = inputIterator.next();
				if (result.isObject()) {
					JsonValue urlValue = result.getAsObject().get("url");
					JsonValue idValue = result.getAsObject().get("package_id");
					if (urlValue.isString() && idValue.isString()) {
						String endpointUri = urlValue.getAsString().value()
								.trim();
						String endpointId = idValue.getAsString().value();

						if (endpointIds.containsKey(endpointId)) {
							System.err.println("id: " + endpointId
									+ " used by multiple endpoints. "
									+ endpointUri + " and "
									+ endpointIds.get(endpointId));
						} else {
							endpointIds.put(endpointId, endpointUri);
						}
					}
				}
				if (max < 1 || i < max) {
					i++;
				} else {
					break;
				}
			}
		}
		return endpointIds.values();
	}

	private Resource readDatasetMetaFromJson(JsonObject jsonRoot,
			JsonObject resource, Model model) {


		JsonValue urlValue = resource.get("url");
		if (!urlValue.isString()) {
			return null;
		}

		String uri = urlValue.getAsString().value().trim();
		Resource datasetResource = null;
		if (uri.length() > 0) {
			String description = resource.get("description").getAsString().value();
			String name = resource.get("name").getAsString().value();
			String title = jsonRoot.get("title").getAsString().value();
			String license_title = jsonRoot.get("license_title").getAsString().value();
			String license = jsonRoot.get("license").getAsString().value();
			String license_id = jsonRoot.get("license_id").getAsString().value();
			String license_url = jsonRoot.get("license_url").getAsString().value();
			String maintainer = jsonRoot.get("maintainer").getAsString().value();
			String maintainer_email = jsonRoot.get("maintainer_email").getAsString().value();
			String author = jsonRoot.get("author").getAsString().value();
			String author_email = jsonRoot.get("author_email").getAsString().value();
			String version = jsonRoot.get("version").getAsString().value();
			String notes_rendered = jsonRoot.get("notes_rendered").getAsString().value();
			String homepage = jsonRoot.get("url").getAsString().value();
			
			datasetResource = model.createResource(uri);
			datasetResource.addProperty(DC.description, description);
			datasetResource.addProperty(DC.title, title);
			datasetResource.addProperty(DCTerms.license, model.createResource(license_url));
			datasetResource.addProperty(DCTerms.creator, "\"" + author + "\" <" + author_email + ">");
			datasetResource.addProperty(DCTerms.created, version);
			datasetResource.addProperty(DC.description, notes_rendered);
			datasetResource.addProperty(FOAF.homepage, homepage);
			
			/*
			 * TODO also extract tags
			 */
		}

		return datasetResource;
	}
}
