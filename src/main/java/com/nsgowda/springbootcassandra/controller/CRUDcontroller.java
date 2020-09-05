package com.nsgowda.springbootcassandra.controller;

import static com.datastax.driver.core.DataType.uuid;
import static com.datastax.driver.core.querybuilder.QueryBuilder.now;
import static com.datastax.driver.core.querybuilder.QueryBuilder.remove;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.utils.UUIDs;
import com.nsgowda.springbootcassandra.service.connector.CassandraConnector;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@RequestMapping("/api")
@Log4j2
public class CRUDcontroller {

  @NonNull private final CassandraConnector connector;

  // swagger mapping
  @ApiOperation(
      value = "Creates a column family",
      httpMethod = "POST",
      notes = "Creates a column family in cassandra")
  @ApiParam(value = "createColumnFamily")
  @PostMapping(value = "/createColumnFamily")
  public void createKeyspace(
      @RequestParam(value = "ColumnFamily") String columnFamilyName,
      @RequestParam(value = "KeyspaceName") String keyspaceName) {
    StringBuilder sb =
        new StringBuilder("CREATE TABLE IF NOT EXISTS ")
            .append(columnFamilyName)
            .append("(")
            .append("id uuid PRIMARY KEY, ")
            .append("title text,")
            .append("subject text);");

    String query = sb.toString();
    Session session = connector.getSession();
    session.execute("use " + keyspaceName + ";");
    session.execute(query);
    log.info("Query executed - {} ", () -> query);
  }

  @PostMapping(value = "/alterTable")
  public void alterTable(
      @RequestParam(value = "ColumnFamily") String columnFamilyName,
      @RequestParam(value = "columnName") String columnName,
      @RequestParam(value = "columnType") String columnType,
      @RequestParam(value = "KeyspaceName") String keyspaceName) {
    StringBuilder sb =
        new StringBuilder("ALTER TABLE ")
            .append(columnFamilyName)
            .append(" ADD ")
            .append(columnName)
            .append(" ")
            .append(columnType)
            .append(";");

    Session session = connector.getSession();
    session.execute("use " + keyspaceName + ";");
    String query = sb.toString();
    session.execute(query);
    log.info("Query executed - {} ", () -> query);
  }

  @PostMapping(value = "/insertData")
  public void createKeyspace(
      @RequestParam(value = "ColumnFamily") String columnFamilyName,
      @RequestParam(value = "KeyspaceName") String keyspaceName,
      @RequestParam(value = "title") String title,
      @RequestParam(value = "subject") String subject) {

    Session session = connector.getSession();
    session.execute("use " + keyspaceName + ";");
    PreparedStatement ps = insertdata(keyspaceName, columnFamilyName, session);
    session.executeAsync(ps.bind(UUIDs.timeBased(), title, subject));
    log.info("Query executed - {} ", () -> ps.bind(UUIDs.timeBased(), title, subject));
  }

  public PreparedStatement insertdata(String keySpace, String columnFamily, Session session) {
    PreparedStatement ps =
        session.prepare(
            String.format(
                "insert into %s.%s (id, title, subject) values (?,?,?)", keySpace, columnFamily));
    ps.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    return ps;
  }

  @GetMapping(value = "/getAllRows")
  public void getgetAllRow(
      @RequestParam(value = "ColumnFamily") String columnFamilyName,
      @RequestParam(value = "KeyspaceName") String keyspaceName) {

    Session session = connector.getSession();
    session.execute("use " + keyspaceName + ";");
    PreparedStatement ps = getRows(keyspaceName, columnFamilyName, session);
    ResultSetFuture future = session.executeAsync(ps.bind());
    future.getUninterruptibly().forEach( r -> System.out.println(r));
  }

  public PreparedStatement getRows(String keySpace, String columnFamily, Session session) {
    PreparedStatement ps =
        session.prepare(
            String.format(
                "select * from %s.%s", keySpace, columnFamily));
    ps.setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    return ps;
  }


}
