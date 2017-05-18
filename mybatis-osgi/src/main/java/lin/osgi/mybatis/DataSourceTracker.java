package lin.osgi.mybatis;

import java.util.HashMap;
import java.util.Hashtable;

import javax.sql.DataSource;

import org.apache.ibatis.session.SqlSessionFactory;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceTracker extends ServiceTracker<DataSource, DataSource> {
	private static final Logger Log = LoggerFactory.getLogger(DataSourceTracker.class);
	
	private static final String DATA_SOURCE_NAME = "dataSourceName";
	
	private HashMap<String, ServiceRegistration<SqlSessionFactory>>
			proxies = new HashMap<>();
	
	public DataSourceTracker(BundleContext context) {
		super(context, DataSource.class, null);
	}
	
	@Override
	public DataSource addingService(ServiceReference<DataSource> reference) {
		
		String dataSourceName = getDataSourceName(reference);
		DataSource ds = context.getService(reference);
		
		register(dataSourceName, ds);
		
		return super.addingService(reference);
	}
	
	@Override
	public void remove(ServiceReference<DataSource> reference) {
		
		String dataSourceName = getDataSourceName(reference);
		unregister(dataSourceName);
		
		super.remove(reference);
	}
	
	private String getDataSourceName(ServiceReference<DataSource> reference) {
		
		String dataSourceName = "";
		Object value = reference.getProperty(DATA_SOURCE_NAME);
		if (value != null) 
			dataSourceName = String.valueOf(value);
		return dataSourceName;
		
	}
	
	private void register(String name, DataSource ds) {
		
		SqlSessionFactoryProxy proxy = new SqlSessionFactoryProxy(name, ds);
		proxy.rebuild(context);
		
		context.addFrameworkListener(proxy);
		
		Hashtable<String, String> props = new Hashtable<>();
		props.put("name", name);
		props.put(DATA_SOURCE_NAME, name);
		ServiceRegistration<SqlSessionFactory> reg =
				context.registerService(SqlSessionFactory.class, proxy, props);
		
		proxies.put(name, reg);
		Log.info("SqlSessionFactory[{}] Registered", name);
		
	}
	
	private void unregister(String name) {
		
		ServiceRegistration<SqlSessionFactory> reg = proxies.remove(name);
		if (reg != null) {
			SqlSessionFactoryProxy proxy = 
					(SqlSessionFactoryProxy)context.getService(reg.getReference());
			context.removeFrameworkListener(proxy);
			Log.info("SqlSessionFactory[{}] Removed", name);
		}
		
	}
	
	@Override
	public void close() {
		for (String name : proxies.keySet()) {
			unregister(name);
		}
		super.close();
	}
	
}
