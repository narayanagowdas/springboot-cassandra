package com.nsgowda.springbootcassandra.controller;

import com.datastax.driver.core.Session;
import com.nsgowda.springbootcassandra.service.connector.CassandraConnector;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@RequestMapping("/api")
@Log4j2
//@Slf4j
public class CreateKeySpaceController {

  @NonNull private final CassandraConnector connector;
  Logger logger = LoggerFactory.getLogger(HelloWorld.class);

  // swagger mapping
  @ApiOperation(
      value = "Creates a keyspace",
      httpMethod = "POST",
      notes = "Creates a key space in cassandra")
  @ApiParam(value = "createsKeyspace")
  @PostMapping(
      value = "/createKeySpace")
  public void createKeyspace(
      @RequestParam(value = "KeyspaceName") String keyspaceName,
      @RequestParam(value = "SimpleStrategy", required = false, defaultValue = "SimpleStrategy")
          String replicationStrategy,
      @RequestParam(value = "replication_factor", required = false, defaultValue = "1")
          Integer replicationFactor) {
    StringBuilder sb =
        new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
            .append(keyspaceName)
            .append(" WITH replication = {")
            .append("'class':'")
            .append(replicationStrategy)
            .append("','replication_factor':")
            .append(replicationFactor)
            .append("};");

    String query = sb.toString();
    Session session = connector.getSession();
    session.execute(query);
    log.info("Query executed - {} ", ()-> query);
//    System.out.println(System.getProperty("user.dir"));
  }
}
