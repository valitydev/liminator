package dev.vality.liminator.config;

import dev.vality.testcontainers.annotations.postgresql.PostgresqlTestcontainerSingleton;
import org.springframework.boot.test.context.SpringBootTest;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@PostgresqlTestcontainerSingleton
@SpringBootTest
public @interface PostgresqlSpringBootITest {
}
