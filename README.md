# Snowflake UID Spring Boot Starter

Spring Boot application integrates `snowflake` quickly, to generate unique identifers in order.

## Quickstart

- Import dependencies

```xml
    <dependency>
        <groupId>com.yookue.springstarter</groupId>
        <artifactId>snowflake-uid-spring-boot-starter</artifactId>
        <version>LATEST</version>
    </dependency>
```

> By default, this starter will auto take effect, you can turn it off by `spring.snowflake-uid.enabled = false`

- Configure Spring Boot `application.yml` with prefix `spring.snowflake-uid` (**Optional**)

```yml
spring:
    snowflake-uid:
        epochPoint: '2022-01-01'
```

- Configure your beans with a `UidGenerator` bean by constructor or `@Autowired`/`@Resource` annotation, then you can access it as you wish.

  There are two different implements of `UidGenerator` for different sinarios:

| Implement Class       | Primary Bean |
|-----------------------|--------------|
| DefaultUidGenerator   | Yes          |
| CacheableUidGenerator | No           |

## Document

- Github: https://github.com/yookue/snowflake-uid-spring-boot-starter
- UID generator github: https://github.com/baidu/uid-generator

## Requirement

- jdk 1.8+

## License

This project is under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)

See the `NOTICE.txt` file for required notices and attributions.

## Donation

You like this package? Then [donate to Yookue](https://yookue.com/public/donate) to support the development.

## Website

- Yookue: https://yookue.com
