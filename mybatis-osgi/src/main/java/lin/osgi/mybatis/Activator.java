package lin.osgi.mybatis;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

public class Activator implements BundleActivator {
	
	private DataSourceTracker tracker;
	
	@Override
	public void start(BundleContext context) throws Exception {
		tracker = new DataSourceTracker(context);
		tracker.open();
	}

	@Override
	public void stop(BundleContext context) throws Exception {
		if (tracker != null)
			tracker.close();
	}

}
