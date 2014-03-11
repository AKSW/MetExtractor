package org.deri.hcls.metex.adapters;

import java.io.IOException;

import org.deri.hcls.Endpoint;
import org.deri.hcls.metex.ExtractorServiceAdapter;

import com.hp.hpl.jena.rdf.model.Model;

public class LinkedDataAdapter implements ExtractorServiceAdapter {

	public LinkedDataAdapter() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Model getMetadata(String endpointUri) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Model getMetadata(Endpoint endpoint) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getServiceLink(String endpointUri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getServiceUri() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isAvailable() {
		// TODO Auto-generated method stub
		return false;
	}

}
