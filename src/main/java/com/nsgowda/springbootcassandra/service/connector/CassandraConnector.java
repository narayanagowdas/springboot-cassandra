package com.nsgowda.springbootcassandra.service.connector;

import com.datastax.driver.core.Session;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class CassandraConnector {

  @NonNull private final Session session;
  //  @NonNull private final Cluster cluster;

  public Session getSession() {
    return this.session;
  }

  public void close() {
    session.close();
    //    cluster.close();
  }
}
