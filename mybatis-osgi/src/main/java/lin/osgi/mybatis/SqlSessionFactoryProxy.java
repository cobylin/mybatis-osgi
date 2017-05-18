package lin.osgi.mybatis;

import java.net.URL;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.binding.MapperRegistry;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;
import org.apache.ibatis.type.Alias;
import org.apache.ibatis.type.MappedJdbcTypes;
import org.apache.ibatis.type.MappedTypes;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.apache.ibatis.type.TypeHandlerRegistry;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.FrameworkListener;
import org.osgi.framework.wiring.BundleWiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SqlSessionFactory 代理，用于当配置变化时重新构造SqlSessionFactory以实现无缝替换
 * 因为MyBatis不支持动态特性，当配置变化时无法设置新的配置内容，必须重新构造实例。e
 */
public class SqlSessionFactoryProxy implements SqlSessionFactory, FrameworkListener {
	private static final Logger Log = LoggerFactory.getLogger(SqlSessionFactoryProxy.class);
	
	private String name;
	private DataSource ds;
	private SqlSessionFactory sqlSessionFactory;
	
	public SqlSessionFactoryProxy(String name, DataSource ds) {
		this.name = name;
		this.ds = ds;
	}
	
	public String getName() {
		return this.name;
	}
	
	public synchronized Configuration rebuild(BundleContext context) {
		
		ManagedTransactionFactory mtf = new ManagedTransactionFactory();
		
		//Properties props = new Properties();
		//props.setProperty("closeConnection", "false");
		//mtf.setProperties(props);
		
		Environment env = new Environment(
							"default",
							mtf,
							ds);
		Configuration conf = new Configuration();
		
		Set<Class<?>> classes = getAllBundleCalss(context);
		regTypeHander(conf, classes);
		regMapper(conf, classes);
		
		conf.setEnvironment(env);
		sqlSessionFactory = new SqlSessionFactoryBuilder().build(conf);
		
		Log.info("SqlSessionFactory Proxy[{}] Rebuilded", name);
		return sqlSessionFactory.getConfiguration();
		
	}
	
	private void regTypeHander(Configuration conf, Set<Class<?>> classes) {
		
		ClassLoader sClassLoader = Thread.currentThread().getContextClassLoader();
		TypeHandlerRegistry typeHandlerRegistry = conf.getTypeHandlerRegistry();
		TypeAliasRegistry typeAliasRegistry = conf.getTypeAliasRegistry();
		
		try {
			
			for (Class<?> c : classes) {
				Thread.currentThread().setContextClassLoader(c.getClassLoader());
				
				if (c.isAnnotationPresent(MappedTypes.class) || c.isAnnotationPresent(MappedJdbcTypes.class)) {
					typeHandlerRegistry.register(c);
					if (c.isAnnotationPresent(Alias.class)) {
						String alias = c.getAnnotation(Alias.class).value();
						typeAliasRegistry.registerAlias(alias, c);
						Log.debug("SqlSessionFactory Proxy[{}] Register TypeHandler And Alias: {} -> {}", name, c, alias);
					} else {
						Log.debug("SqlSessionFactory Proxy[{}] Register TypeHandler: {}", name, c);
					}
				}
			}
		} finally {
			Thread.currentThread().setContextClassLoader(sClassLoader);
		}
		
	}
	
	private void regMapper(Configuration conf, Set<Class<?>> classes) {
		
		ClassLoader sClassLoader = Thread.currentThread().getContextClassLoader();
		MapperRegistry mapperRegistry = conf.getMapperRegistry();
		
		try {
			for (Class<?> c : classes) {
				Thread.currentThread().setContextClassLoader(c.getClassLoader());
				
				if (c.isInterface() && c.isAnnotationPresent(Mapper.class)) {
					try {
						String xmlMapperName = c.getSimpleName() + ".xml";
						URL xmlMapper = c.getResource(xmlMapperName);
						
						//if not mapper xml then use ann
						if (Objects.isNull(xmlMapper)) {
							mapperRegistry.addMapper(c);
							Log.debug("SqlSessionFactory Proxy[{}] Add Mapper: {}", name, c);
						}
						else {
							XMLMapperBuilder xmlMapperBuild = new XMLMapperBuilder(
																xmlMapper.openStream(),
																conf,
																xmlMapper.toString(),
																conf.getSqlFragments());
							xmlMapperBuild.parse();
							Log.debug("SqlSessionFactory Proxy[{}] Add Mapper XML: {}", name, xmlMapper);
						}
					} catch (Exception e) {
						Log.warn("Cannot Add Mapper: {}", c, e);
					}
					
				}
			}
		} finally {
			Thread.currentThread().setContextClassLoader(sClassLoader);
		}
	}
	
	@Override
	public SqlSession openSession() {
		return sqlSessionFactory.openSession();
	}

	@Override
	public SqlSession openSession(boolean autoCommit) {
		return sqlSessionFactory.openSession(autoCommit);
	}

	@Override
	public SqlSession openSession(Connection connection) {
		return sqlSessionFactory.openSession(connection);
	}

	@Override
	public SqlSession openSession(TransactionIsolationLevel level) {
		return sqlSessionFactory.openSession(level);
	}

	@Override
	public SqlSession openSession(ExecutorType execType) {
		return sqlSessionFactory.openSession(execType);
	}

	@Override
	public SqlSession openSession(ExecutorType execType, boolean autoCommit) {
		return sqlSessionFactory.openSession(execType, autoCommit);
	}

	@Override
	public SqlSession openSession(ExecutorType execType, TransactionIsolationLevel level) {
		return sqlSessionFactory.openSession(execType, level);
	}

	@Override
	public SqlSession openSession(ExecutorType execType, Connection connection) {
		return sqlSessionFactory.openSession(execType, connection);
	}

	@Override
	public Configuration getConfiguration() {
		return sqlSessionFactory.getConfiguration();
	}

	@Override
	public void frameworkEvent(FrameworkEvent event) {
		BundleContext context = event.getBundle().getBundleContext();
		if (event.getType() == FrameworkEvent.PACKAGES_REFRESHED)
			rebuild(context);
	}
	
	
	private Set<Class<?>> getAllBundleCalss(BundleContext context) {
		
		Set<Class<?>> allBundleClasses = new HashSet<>();
		for (Bundle bundle : context.getBundles()) {
			if (isMyBatisScanBundle(bundle) && isActiveBundle(bundle)) {
				allBundleClasses.addAll(getBundleClass(bundle));
			}
		}
		return allBundleClasses;
		
	}
	
	private Set<Class<?>> getBundleClass(Bundle bundle) {
		
		BundleWiring wiring = bundle.adapt(BundleWiring.class);
		
		Collection<String> localResources;
		localResources = wiring.listResources("/", null, BundleWiring.LISTRESOURCES_RECURSE | BundleWiring.LISTRESOURCES_LOCAL);
		
		Set<Class<?>> classes = new HashSet<>();
		localResources.forEach( c -> {
			if (c.endsWith(".class")) {
        		c = c.substring(0, c.length() - 6).replace('/', '.');
        		Class<?> clazz;
				try {
					clazz = bundle.loadClass(c);
					classes.add(clazz);
				} catch (Exception e) {
					Log.warn("Cannot Scan Class: {}", c, e);
				}
			}
		});
		
		return classes;
	}
	
	protected boolean isMyBatisScanBundle(Bundle bundle) {
		return "true".equals(bundle.getHeaders().get("MyBatis-Scan"));
	}
	
	protected boolean isActiveBundle(Bundle bundle) {
		int state = bundle.getState();
		return state >= Bundle.RESOLVED;
	}

}
