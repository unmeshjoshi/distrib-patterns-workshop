# Distributed Patterns Workshop - Build Structure

This repository contains a multi-day workshop on distributed systems patterns, organized as a multi-project Gradle build.

## Project Structure

```
distrib-patterns-workshop/
├── build.gradle          # Root build file with common dependencies
├── settings.gradle       # Root settings and subproject declarations
├── gradlew              # Gradle wrapper for Unix/Mac
├── gradlew.bat          # Gradle wrapper for Windows
├── gradle/              # Gradle wrapper JAR and properties
├── day1/
│   ├── section01-littles-law/
│   ├── section02-failures/
│   ├── section03-wal/   # WAL subproject (Java/Gradle)
│   │   ├── build.gradle # WAL-specific configuration
│   │   └── src/         # Source code
│   └── section04-quorums/
└── day2/                # Future day 2 sections
    └── ...
```

## Root Build Configuration

The root `build.gradle` defines common dependencies and configuration for all Java subprojects:

### Common Dependencies
- **Tickloom** - Library for distributed patterns (includes Jackson core and databind)
- **Jackson** - Additional modules (datatype-jdk8, module-parameter-names)
- **Log4j** - Logging framework (api, core)
- **JUnit** - Testing framework

### Common Configuration
- Java 11 source and target compatibility
- Compiler arguments for Jackson parameter names
- Test logging configuration (passed, skipped, failed tests with full exception format)
- Maven Central repository

## Building

### Build all subprojects
```bash
./gradlew build
```

### Build specific day or section
```bash
./gradlew :day1:section03-wal:build
```

### Run tests
```bash
# All tests
./gradlew test

# Specific section
./gradlew :day1:section03-wal:test
```

### List all projects
```bash
./gradlew projects
```

### Clean build
```bash
./gradlew clean build
```

## Adding New Subprojects

### Adding a Day 1 Section

1. Create the section directory under `day1/`
2. For Java projects, create `day1/section-name/build.gradle`:
   ```groovy
   // Section-specific dependencies
   dependencies {
       // Add section-specific dependencies here
       // Common dependencies are inherited from root
   }
   ```
3. Add to `settings.gradle`:
   ```groovy
   include 'day1:section-name'
   ```

### Adding Day 2 Sections

1. Create `day2/` directory (if not exists)
2. Create section directories under `day2/`
3. Add to `settings.gradle`:
   ```groovy
   include 'day2:section01-...'
   include 'day2:section02-...'
   ```

## Current Sections

### Day 1

- **section01-littles-law** - Little's Law demonstrations (shell scripts)
- **section02-failures** - Failure mathematics (Python)
- **section03-wal** - Write-Ahead Log implementation (Java)
  - Implements WAL patterns for durability
  - Performance comparison tests
  - Optimized WAL with batching and fsync
- **section04-quorums** - Quorum-based consensus (Python)

## Benefits of This Structure

✅ **Centralized Dependencies** - All common dependencies managed in one place  
✅ **Consistent Configuration** - Java version, compiler settings, test configuration shared  
✅ **Easy Expansion** - Add new days and sections easily  
✅ **Selective Building** - Build individual sections or entire days  
✅ **Single Wrapper** - One gradle wrapper at root manages all projects  

## Migration Notes

This structure was created to support multiple workshop days. The original `section03-wal` was a standalone Gradle project that has been integrated as a subproject. All common dependencies were moved to the root build file.

