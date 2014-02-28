package org.deri.hcls.metex;

import java.io.IOException;
import java.util.Collection;
import ie.deri.hcls.Endpoint;

import com.hp.hpl.jena.rdf.model.Model;

public interface WebServiceAdapter {
	public Model getMetadata(String endpoint) throws IOException;
	public Model getMetadata(Endpoint endpoint) throws IOException;
	public String getTitle(String endpoint);
	public Collection<String> getAllEndpoints() throws IOException;
}
