package bigdatarocks.common.dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.apache.log4j.Logger;

/**
 * Created by ashe on 06/05/16.
 */
public abstract class ACassandraDao<T> {
    private static final Logger LOGGER = Logger.getLogger(ACassandraDao.class);

    private String nodes;
    private String port;
    private String keyspace;
    private Cluster cluster;
    protected Session session;
    protected Mapper<T> mapper;

    public ACassandraDao(String nodes, String port, String keyspace) {
        this.nodes = nodes;
        this.port = port;
        this.keyspace = keyspace;
    }

    public void init(Class<T> tClass) {
        try {
            Cluster.Builder builder = Cluster.builder();
            final String[] nodesList = nodes.split(",");
            for (String node : nodesList) {
                builder.addContactPoint(node).withPort(Integer.parseInt(port));
                LOGGER.info(String.format("Cassandra adding node: %s", node));
            }
            cluster = builder.build();
            session = null;
            if (keyspace != null) {
                session = cluster.connect(keyspace);
            } else {
                session = cluster.connect();
            }
            MappingManager mappingManager = new MappingManager(session);
            mapper = mappingManager.mapper(tClass);
        } catch (Exception e) {
            LOGGER.error("Error initializing CassandraDao");
            throw e;
        }
    }

    public synchronized void close() {
        if (this.session != null) {
            this.session.close();
        }
        if (this.cluster != null) {
            try {
                this.cluster.close();
            } catch (Exception e) {
                LOGGER.error("Erro closing CassandraDao");
                throw e;
            }
        }
    }

}
