package org.deri.hcls.metex;

import java.io.IOException;
import java.util.Collection;

public interface EndpointListProviderAdapter {
	public Collection<String> getAllEndpoints() throws IOException;
	public Collection<String> getSomeEndpoints(int limit) throws IOException;
}
