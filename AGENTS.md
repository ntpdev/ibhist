# AGENTS.md
This file provides important information about the project for coding agents

## Description

A Java CLI application that connects to Interactive Brokers (IB) via the TWS (Trader Workstation) API. The application supports multiple operational modes including historical data retrieval, real-time market data streaming, and an interactive REPL for command-line interaction.

The application uses CSV files and MongoDB for persistent storage of time series data, Guice for dependency injection, and integrates with the Interactive Brokers trading platform.

The TWS API is asynchronous in nature. Requests are made on one thread and the response are sent back ansynchronously over a socket connection. IBConnector handles sending the requests and EWrapperImpl processes the responses. Messages are modelled using the XxxAction classes.

The main data structure is PriceHistory.java which holds timeseries data as columns along with calculated indicators. The data can be loaded from csv files via a PriceHistoryRepository or from MongoDB timeseries via TimeSeriesRepository.

**Features:**
- Historical data processing (`hist` mode)
- ES day data processing (`day` mode - default)
- Real-time bar data streaming (`rt` mode)
- Interactive REPL mode (`repl`)
- MongoDB and CSV files for time series data.

## Environment
- Java JDK 25
- Gradle 8 with Kotlin DSL
- Guice for dependency injection
- MongoDB 8 via the native MongoDB Java database driver 5.6.x
- Log4j2 is used for logging

## Directory Structure

```
c:/code/ibhist/
├── app/
│   ├── build.gradle.kts          # Application build configuration
│   ├── lib/                      # Local JAR dependencies (TWS API jars)
│   └── src/
│       ├── main/
│       │   ├── java/ibhist/      # Main source code
│       │   │   ├── App.java      # Application entry point
│       │   │   ├── AppModule.java # Guice module
│       │   │   ├── EWrapperImpl.java # The subclass that receives asynchronous call backs from TWS
│       │   │   ├── IBConnector.java / IBConnectorImpl.java
│       │   │   ├── Repl.java / ReplImpl.java
│       │   │   ├── TimeSeriesRepository.java / TimeSeriesRepositoryImpl.java
│       │   │   ├── PriceHistory.java / PriceHistoryRepository.java
│       │   │   ├── RealTimeBar.java / RealTimeHistory.java
│       │   │   ├── OrderBuilder.java / OrderDetails.java
│       │   │   ├── HistoricalDataAction.java / RealTimeBarsAction.java
│       │   │   ├── ContractFactory.java / ContractFactoryImpl.java
│       │   │   └── ...           # Other utility classes
│       │   └── resources/
│       │       └── log4j2.xml    # Log4j2 logging configuration
│       └── test/
│           ├── java/ibhist/      # Unit tests
│           │   └── ...           # Other test classes
│           └── resources/
│               └── log4j2.xml    # Test logging configuration
├── settings.gradle.kts           # Gradle project settings
├── gradlew                       # Gradle wrapper (Unix)
├── gradlew.bat                   # Gradle wrapper (Windows)
├── README.md                     # Quick reference and commands
└── AGENTS.md                # This file
```

## Naming Conventions
Follow standard Java naming patterns for classes and variable. Main services are injected using Guice and are named MyService for the interface and MyServiceImpl for the implementation.

**TEST** methods should be named using longer lowercase names that describe the scenario. Example - moving_average_calculation

**Build Commands:**

| Command | Description |
|---------|-------------|
| `./gradlew run` | Run the application (defaults to `day` mode) |
| `./gradlew run --args="repl"` | Run with REPL mode |
| `./gradlew run --args="hist"` | Run in historical data mode |
| `./gradlew run --args="rt"` | Run in real-time data mode |
| `./gradlew installDist` | Create install distribution |
| `./gradlew test` | Run all unit tests |
| `./gradlew build` | Build the project (compile + test) |


## Java Coding Style

Use modern Java 25 features where appropriate. Prefer these patterns:

1. **Records** for immutable data classes (`RealTimeBar`, `OrderDetails`, action classes). If a class is just fields + accessors with no mutable state, it should be a record.

2. **Sealed interfaces/classes** for closed type hierarchies. The `XxxAction` classes are a natural fit — a sealed interface with a permitted set of action records makes the dispatch exhaustive and self-documenting.

3. **Pattern matching for `instanceof`** — replace `if (x instanceof Foo) { Foo f = (Foo) x; ... }` with `if (x instanceof Foo f) { ... }`.

4. **Switch expressions with pattern matching** — use `switch` as an expression (with `->` arms) rather than chains of `if/else`. Especially useful when dispatching on sealed action types.

5. **Text blocks** for any multi-line strings — SQL queries, JSON templates, log messages with structure. No more string concatenation or `StringBuilder` for these.

6. **`var`** for local variables where the type is obvious from the right-hand side. Don't use it when it would obscure the type (e.g. return values from generic methods). Do not use var for value types like int or String.

7. **Named patterns / record patterns** — deconstruct records directly in `switch` or `instanceof` checks rather than calling accessors manually.

8. **Prefer `List.of()` / `Map.of()`** for immutable collections over `Collections.unmodifiableList()` or ad-hoc construction. Use `List.copyOf()` when you need an immutable view of a mutable collection.

9. **Avoid raw or legacy exception patterns** — prefer specific checked exceptions or well-typed unchecked exceptions over broad `catch (Exception e)`. Don't swallow exceptions silently.

10. **`Optional` for nullable returns** — methods that may legitimately return no value should return `Optional<T>` rather than `null`, particularly in repository and factory interfaces.

11. **Ternary operator for simple conditional expressions** — use `condition ? valueA : valueB` where it keeps a single expression readable. Don't use it for complex or nested conditions; if you need more than one line to understand it, use a regular `if/else`.

12. **`@Nullable` on parameters and return types that can be null** — use `org.jetbrains.annotations.Nullable` to explicitly mark these. The default assumption across the codebase is that parameters are non-null; null checks should be avoided unless a parameter is annotated. When a nullable value is received, handle or guard it at the boundary rather than threading null through the call chain.

## Libraries

### Runtime Dependencies

- org.jetbrains:annotations Nullability annotations
- com.google.guava:guava latest Google's core Java libraries
- com.google.inject:guice Dependency injection framework
- org.apache.logging.log4j:log4j-core Logging framework
- org.mongodb:mongodb-driver-sync MongoDB database driver 5.6
- TWS API JARs - Interactive Brokers Trading API

### Test Dependencies

- org.junit.jupiter:junit-jupiter JUnit 5 testing framework
- org.assertj:assertj-core AssertJ 3.27 Fluent assertion library
- org.mockito:mockito-core Mocking framework

## MongoDB Timeseries collections

**Symbols:** The `symbol` field is always lowercase. There are two types of symbol: futures contracts and index data. Futures symbols use standard CME notation — the root symbol followed by the expiry month letter and single-digit year. For example, `esz5` is the ES (E-mini S&P 500) December 2025 contract (`z` = December, `5` = 2025). Month codes are: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun, N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec. Index symbols are plain names like `tick-nyse` with no expiry encoding.

**Futures trading context:** CME futures trade nearly 24 hours a day, Sunday evening through Friday afternoon US Eastern time. A "trade date" is not a calendar date — it is the logical trading day as defined by CME. For example, the ES session for trade date Monday 2025-03-10 actually starts Sunday evening 2025-03-09 at 23:00 UTC. The `tradeDate` field represents this logical CME trade date, not the calendar date the bars fall on. RTH (Regular Trading Hours) is the subset of the session that corresponds to the main equity market hours (9:30–16:00 US Eastern), as distinct from the extended overnight/globex session. Index data follows standard exchange hours and does not have the overnight session complexity.

**Data flow:**

| Collection | timeField | metaField | Granularity | Contains | Description |
|---|---|---|---|---|---|
| `m1` | `timestamp` | `symbol` | minutes | futures + index | Source of truth. 1-minute OHLCV bars for all symbols. All other collections are derived from this. |
| `tradeDate` | `tradeDate` | `symbol` | hours | futures + index | Index of contiguous trading sessions. Built by scanning `m1` for gaps > 30 mins. Includes RTH start/end times. Unique index on `(symbol, tradeDate)`. |
| `daily` | `tradeDate` | `symbol` | hours | futures only | Daily OHLCV bars aggregated from `m1` using full session boundaries. |
| `dailyRth` | `tradeDate` | `symbol` | hours | futures only | Daily OHLCV bars aggregated from `m1` using RTH boundaries only. |

CSV or IB API ───→ m1 ──────────────→ daily, dailyRTH
                   │                 ▲
                   └──→ tradeDate ───┘

Historical raw time series data at 1 minute resolution is loaded from CSV files into the m1 collection. The tradeDate collection is then populated with the timestamps for each trading days and symbol. For futures contracts, the m1 data is aggregated into the daily and dailyRth collections.

### External Resources

- [Interactive Brokers API Documentation](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/)
- [TWS API GitHub](https://interactivebrokers.github.io/)
