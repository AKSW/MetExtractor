package org.aksw.metex.adapters;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.aksw.metex.EndpointListProviderAdapter;
import org.aksw.metex.ExtractorServiceAdapter;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.deri.hcls.Endpoint;
import org.deri.hcls.vocabulary.VOID;
import org.deri.hcls.vocabulary.Vocabularies;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.vocabulary.FOAF;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

public class DatahubAdapter implements ExtractorServiceAdapter,
		EndpointListProviderAdapter {

	private static final String API_BASE = "http://datahub.io/api/2/";
	private static final String ENDPOINT_LIST_URL = API_BASE
			+ "search/resource?format=api/sparql&all_fields=1&limit=1000";
	private static final String DATASET_BASE = API_BASE + "rest/dataset/";
	private static final String RESOURCE_SEARCH_BASE = API_BASE
			+ "search/resource";
	private static final String TAG_PREFIX = "http://datahub.io/dataset?tags=";

	private static Map<String, String> endpointIds = new HashMap<String, String>();
	private static Map<String, String> packageIds = new HashMap<String, String>();

	public DatahubAdapter() {
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		return getMetadata(endpoint.getUri());
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		String endpointId = getPackageId(endpointUri);
		if (endpointId == null) {
			/*
			 * There is no data available at datahub for this endpoint
			 */
			return null;
		}

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
						Resource datasetResource = readDatasetMetaFromJson(
								metadataJson, resource.getAsObject(), model);
						Resource endpointResource = model
								.createResource(endpointUri);
						endpointResource
								.addProperty(Vocabularies.SD_defaultDataset,
										datasetResource);
						datasetResource.addProperty(RDF.type, VOID.Dataset);
						datasetResource.addProperty(VOID.sparqlEndpoint,
								endpointResource);
					}
				}
			}
		}

		return model;
	}

	@Override
	public Collection<String> getSomeEndpoints(int limit) throws IOException {
		return getEndpoints(limit);
	}

	@Override
	public Collection<String> getAllEndpoints() throws IOException {
		return getEndpoints(0);
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
					JsonValue idValue = result.getAsObject().get("id");
					JsonValue pidValue = result.getAsObject().get("package_id");
					if (urlValue.isString() && pidValue.isString()
							&& idValue.isString()) {
						String endpointUri = urlValue.getAsString().value()
								.trim();
						String endpointId = idValue.getAsString().value();
						String packageId = pidValue.getAsString().value();

						if (endpointIds.containsKey(endpointId)) {
							System.err.println("id: " + endpointId
									+ " used by multiple endpoints. "
									+ endpointUri + " and ?");
						} else {
							endpointIds.put(endpointUri, endpointId);
						}

						if (packageIds.containsKey(packageId)) {
							System.err.println("pid: " + packageId
									+ " used by multiple endpoints. "
									+ endpointUri + " and ?");
						} else {
							packageIds.put(endpointUri, packageId);
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
		return endpointIds.keySet();
	}

	private String getEndpointId(String uri) {
		if (endpointIds.isEmpty()) {
			getIds(uri);
		}
		return endpointIds.get(uri);
	}

	private String getPackageId(String uri) {
		if (packageIds.isEmpty()) {
			getIds(uri);
		}
		return packageIds.get(uri);
	}

	private void getIds(String uri) {
		try {
			URL url = new URL(RESOURCE_SEARCH_BASE + "?url="
					+ URLEncoder.encode(uri, "UTF-8") + "&all_fields=1");
			URLConnection conn = url.openConnection();
			InputStream inputstream = conn.getInputStream();

			JsonObject resultObject = JSON.parse(inputstream);
			JsonObject result = resultObject.get("results").getAsArray().get(0)
					.getAsObject();
			String id = result.get("id").getAsString().value();
			String pid = result.get("package_id").getAsString().value();
			endpointIds.put(uri, id);
			packageIds.put(uri, pid);
		} catch (Exception e) {
			try {
				getAllEndpoints();
			} catch (IOException ee) {
			}
		}
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
			JsonObject extras = jsonRoot.get("extras").getAsObject();

			datasetResource = model.createResource(uri);

			setStringPropertyFromJson(datasetResource, DC.description,
					resource.get("description"));
			// setStringPropertyFromJson(datasetResource, DC.title,
			// jsonRoot.get("name"));
			setStringPropertyFromJson(datasetResource, DC.title,
					jsonRoot.get("title"));

			// setStringPropertyFromJson(datasetResource, DCTerms.license,
			// jsonRoot.get("license_title"));
			// setStringPropertyFromJson(datasetResource, DCTerms.license,
			// jsonRoot.get("license"));
			// setStringPropertyFromJson(datasetResource, DCTerms.license,
			// jsonRoot.get("license_id"));
			setResourcePropertyFromJson(datasetResource, DCTerms.license,
					jsonRoot.get("license_url"));

			setStringPropertyFromJson(datasetResource, DCTerms.created,
					jsonRoot.get("version"));
			setStringPropertyFromJson(datasetResource, DC.description,
					jsonRoot.get("notes_rendered"));
			setResourcePropertyFromJson(datasetResource, FOAF.homepage,
					jsonRoot.get("url"));
			setIntPropertyFromJson(datasetResource, VOID.triples,
					extras.get("triples"));
			setResourcePropertyFromJson(datasetResource, VOID.uriSpace,
					extras.get("namespace"));

			try {
				String creatorString;

				if (jsonRoot.hasKey("author")
						&& jsonRoot.hasKey("author_email")) {
					String author = jsonRoot.get("author").getAsString()
							.value();
					String author_email = jsonRoot.get("author_email")
							.getAsString().value();

					creatorString = "\"" + author + "\" <" + author_email + ">";
				} else if (jsonRoot.hasKey("author")
						&& jsonRoot.hasKey("author_email")) {

					String maintainer = jsonRoot.get("maintainer")
							.getAsString().value();
					String maintainer_email = jsonRoot.get("maintainer_email")
							.getAsString().value();

					creatorString = "\"" + maintainer + "\" <"
							+ maintainer_email + ">";
				} else {
					// will be caught
					creatorString = null;
				}

				datasetResource.addProperty(DCTerms.creator, creatorString);
			} catch (Exception e) {
			}

			try {
				JsonArray tags = jsonRoot.get("tags").getAsArray();
				for (JsonValue tag : tags) {
					String tagValue = tag.getAsString().value();
					Resource tagResource = model.createResource(TAG_PREFIX
							+ tagValue);
					datasetResource.addProperty(DCTerms.subject, tagResource);
				}
			} catch (Exception e) {
			}
		}

		return datasetResource;
	}

	private void setStringPropertyFromJson(Resource resource,
			Property property, JsonValue jsonValue) {
		try {
			String value = jsonValue.getAsString().value();
			resource.addProperty(property, value);
		} catch (Exception e) {
		}
	}

	private void setIntPropertyFromJson(Resource resource, Property property,
			JsonValue jsonValue) {
		try {
			int value = jsonValue.getAsNumber().value().intValue();

			Model model = resource.getModel();
			Literal literal = model.createTypedLiteral(value);
			resource.addProperty(property, literal);
		} catch (Exception e) {
		}
	}

	private void setResourcePropertyFromJson(Resource resource,
			Property property, JsonValue jsonValue) {
		try {
			String value = jsonValue.getAsString().value();

			Model model = resource.getModel();
			Resource valueResource = model.createResource(value);
			resource.addProperty(property, valueResource);
		} catch (Exception e) {
		}
	}

	@Override
	public String getServiceLink(String endpointUri) {
		String rId = getEndpointId(endpointUri);
		String pId = getPackageId(endpointUri);
		try {
			URL url = new URL(DATASET_BASE + pId);
			URLConnection conn = url.openConnection();
			InputStream inputstream = conn.getInputStream();

			JsonObject datasetObject = JSON.parse(inputstream);
			String ckanUrl = datasetObject.get("ckan_url").getAsString()
					.value();

			return ckanUrl + "/resource/" + rId;
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public String getServiceUri() {
		return "http://datahub.io/";
	}

	@Override
	public boolean isAvailable() {
		try {
			URL url = new URL(API_BASE);
			URLConnection conn = url.openConnection();
			InputStream inputstream = conn.getInputStream();

			JsonObject versionObject = JSON.parse(inputstream);
			int version = versionObject.get("version").getAsNumber().value()
					.intValue();
			if (version == 2) {
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			return false;
		}
	}
}
