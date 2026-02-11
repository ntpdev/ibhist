# AGENTS.md
This file provides important information about the project for coding agents

## Description

A Java CLI application that connects to Interactive Brokers (IB) via the TWS (Trader Workstation) API. The application supports multiple operational modes including historical data retrieval, real-time market data streaming, and an interactive REPL for command-line interaction.

The application uses CSV files and MongoDB for persistent storage of time series data, Guice for dependency injection, and integrates with the Interactive Brokers trading platform.

The TWS API is asynchronous in nature. Requests are made on one thread and responses are sent back asynchronously over a socket connection. IBConnector handles sending the requests and EWrapperImpl processes the responses. Messages are modelled using the XxxAction classes.

The main data structure is PriceHistory.java which holds timeseries data as columns along with calculated indicators. Data can be loaded from CSV files via PriceHistoryRepository or from MongoDB timeseries via TimeSeriesRepository.

## Environment
- Java JDK 25
- Gradle 8 with Kotlin DSL
- Guice for dependency injection
- MongoDB 8 via native MongoDB Java driver 5.6.x
- Log4j2 for logging

## Build Commands

| Command | Description |
|---------|-------------|
| `./gradlew build` | Build project (compile + test) |
| `./gradlew test` | Run all unit tests |
| `./gradlew test --tests "PriceHistoryTest"` | Run single test class |
| `./gradlew test --tests "PriceHistoryTest.ema_step"` | Run single test method |
| `./gradlew run` | Run application (defaults to `day` mode) |
| `./gradlew run --args="repl"` | Run with REPL mode |
| `./gradlew run --args="hist"` | Run in historical data mode |
| `./gradlew run --args="rt"` | Run in real-time data mode |
| `./gradlew installDist` | Create install distribution |

## Directory Structure

```
c:/code/ibhist/
├── app/
│   ├── build.gradle.kts          # Application build configuration
│   ├── lib/                      # Local JAR dependencies (TWS API jars)
│   └── src/
│       ├── main/java/ibhist/     # Main source code
│       │   ├── App.java          # Application entry point
│       │   ├── AppModule.java    # Guice module
│       │   ├── EWrapperImpl.java # Async callbacks from TWS
│       │   ├── IBConnector.java  # Interface for IB connection
│       │   ├── IBConnectorImpl.java
│       │   ├── Action.java / ActionBase.java / XxxAction.java
│       │   ├── PriceHistory.java # Core timeseries data structure
│       │   ├── TimeSeriesRepository.java / TimeSeriesRepositoryImpl.java
│       │   └── ...
│       └── test/java/ibhist/     # Unit tests
├── settings.gradle.kts           # Gradle project settings
└── README.md
```

## Java Coding Style

### Modern Java Features (JDK 25)

1. **Records** for immutable data classes (`RealTimeBar`, `OrderDetails`, action classes). If a class is just fields + accessors with no mutable state, make it a record.

2. **Sealed interfaces/classes** for closed type hierarchies. The `XxxAction` classes are candidates for sealed interfaces.

3. **Pattern matching for `instanceof`** — use `if (x instanceof Foo f) { ... }` instead of casting.

4. **Switch expressions with `->` arms** — prefer over `if/else` chains, especially for sealed types.

5. **Text blocks** for multi-line strings (SQL, JSON, formatted output).

6. **`var`** for local variables where type is obvious from RHS. Do NOT use for value types (int, String) or when type would be obscured.

7. **`List.of()` / `Map.of()`** for immutable collections. Use `List.copyOf()` for immutable views.

8. **`Optional<T>`** for nullable returns — never return `null` from repository/factory interfaces.

### Naming Conventions

- **Classes**: `PascalCase`. Services: `MyService` interface, `MyServiceImpl` implementation.
- **Methods**: `camelCase`. Use descriptive names that read well in tests.
- **Test methods**: `snake_case` describing scenario, e.g., `ema_step`, `local_max_decreasing`.
- **Constants**: `SCREAMING_SNAKE_CASE` or `camelCase` for private static finals.

### Imports

- Import specific classes, not `.*` wildcards.
- Order: java.*, javax.*, third-party (com.*, org.*), project packages.
- Static imports: use for assertion methods and constants.

### Nullability

- Default assumption: parameters are non-null.
- Use `@Nullable` (org.jetbrains.annotations.Nullable) for nullable parameters/returns.
- Guard nullable values at boundaries, don't thread null through call chain.

### Error Handling

- Throw specific exceptions with descriptive messages.
- Don't catch broad `Exception` or swallow exceptions silently.
- Log errors with context before rethrowing when appropriate.

### Comments

- Avoid inline comments explaining "what" — code should be self-documenting.
- Javadoc on public APIs where behavior isn't obvious from signature.
- Use TODO sparingly, with context.

## Libraries

### Runtime
- org.jetbrains:annotations — Nullability annotations
- com.google.guava:guava — Core utilities
- com.google.inject:guice — Dependency injection
- org.apache.logging.log4j:log4j-core — Logging
- org.mongodb:mongodb-driver-sync:5.6 — MongoDB driver
- TWS API JARs (local) — Interactive Brokers API

### Test
- org.junit.jupiter:junit-jupiter:5.14 — JUnit 5
- org.assertj:assertj-core:3.27 — Fluent assertions
- org.mockito:mockito-core:5.21 — Mocking

## MongoDB Collections

| Collection | timeField | metaField | Granularity | Description |
|------------|-----------|-----------|-------------|-------------|
| `m1` | `timestamp` | `symbol` | minutes | Source of truth. 1-min OHLCV bars. |
| `tradeDate` | `tradeDate` | `symbol` | hours | Trading session index with RTH bounds. |
| `daily` | `tradeDate` | `symbol` | hours | Daily OHLCV (full session). |
| `dailyRth` | `tradeDate` | `symbol` | hours | Daily OHLCV (RTH only). |

**Symbols**: Lowercase. Futures use CME notation: `esz5` = ES Dec 2025 (`z`=Dec, `5`=2025). Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec.

**Trade Date**: CME logical trading day, not calendar date. Session starts previous evening (e.g., Monday trade date starts Sunday 23:00 UTC).

## External Resources

- [Interactive Brokers API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/)
- [TWS API GitHub](https://interactivebrokers.github.io/)
