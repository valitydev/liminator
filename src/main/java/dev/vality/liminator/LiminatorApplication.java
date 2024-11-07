package dev.vality.liminator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@ServletComponentScan
@SpringBootApplication(scanBasePackages = {"dev.vality.liminator"})
public class LiminatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(LiminatorApplication.class, args);
    }
}
