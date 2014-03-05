package org.deri.hcls.metex;

import java.io.IOException;
import java.util.Collection;

import org.deri.hcls.Endpoint;

import com.hp.hpl.jena.rdf.model.Model;

public interface ExtractorServiceAdapter {
	public Model getMetadata(String endpointUri) throws IOException;

	public Model getMetadata(String endpointUri, Collection<String> properties)
			throws IOException;

	public Model getMetadata(Endpoint endpoint) throws IOException;

	public Model getMetadata(Endpoint endpoint, Collection<String> properties)
			throws IOException;

	public String getServiceLink(String endpointUri);
	
	public String getServiceUri();
	
	public boolean isAvailable();
}
