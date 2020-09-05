package com.nsgowda.springbootcassandra.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class HelloWorld {

  Logger logger = LoggerFactory.getLogger(HelloWorld.class);

  @GetMapping("/hello")
  public String greeting() {
    logger.trace("Hello world is called!");
    return "Hello world!";
  }
}
