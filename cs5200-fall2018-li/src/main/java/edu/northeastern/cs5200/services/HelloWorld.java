package edu.northeastern.cs5200.services;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloWorld {
	@GetMapping("/hello")
	public String sayHello(){
		return "hello world";
	}

}
