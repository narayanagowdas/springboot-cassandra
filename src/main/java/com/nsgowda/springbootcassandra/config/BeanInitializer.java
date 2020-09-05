package com.nsgowda.springbootcassandra.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class BeanInitializer {

  private static String host = "localhost";
  private static int port = 9042;
  private static String userName = "cassandra";
  private static String password = "cassandra";
  private static String clusterName = "easybuy";
  private static int MAX_CONNECTIONS = 10;
  private static int CORE_CONNECTIONS = 2;

  private static Cluster cluster;
  private static Session session;

  //  @Bean
  //  public Session session() {
  //    return cluster(node, port).connect();
  //  }
  //
  //  public Cluster cluster(String node, int port) {
  //    Cluster.Builder b = Cluster.builder().addContactPoint(node);
  //    if (port != 0) {
  //      b.withPort(port);
  //    }
  //    b.withoutJMXReporting();
  //    //    b.withProtocolVersion(ProtocolVersion.V4);
  //    return b.build();
  //  }

  @Bean
  public Session session() {
    // We are configuring the connection pool
    PoolingOptions poolingOptions = new PoolingOptions();
    // no of max connections
    poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, MAX_CONNECTIONS);
    // no of min connections this host will be allotted
    poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, CORE_CONNECTIONS);

    // Create a cluster object
    cluster =
        Cluster.builder()
            .addContactPoint(host)
            .withPort(port)
            //        .withCredentials(userName, password)
            .withPoolingOptions(poolingOptions)
            .withClusterName(clusterName)
            .withoutJMXReporting()
            .build();

    final Metadata metadata = cluster.getMetadata();
    System.out.println("Connected to cluster: " + metadata.getClusterName());
    // printHostNames(metadata);
    session = cluster.connect();

    return session;
  }
}
