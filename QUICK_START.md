# Quick Start Guide

## Building Projects

All commands should be run from the **root directory** (`distrib-patterns-workshop/`).

### Common Commands

```bash
# Build everything
./gradlew build

# Clean and build
./gradlew clean build

# Run all tests
./gradlew test

# List all projects
./gradlew projects

# See available tasks
./gradlew tasks
```

### Working with Specific Sections

```bash
# Build only day1/section03-wal
./gradlew :day1:section03-wal:build

# Run tests for day1/section03-wal
./gradlew :day1:section03-wal:test

# Clean a specific section
./gradlew :day1:section03-wal:clean
```

## Project Hierarchy

```
distrib-patterns-workshop/           (root)
├── :day1                            (day1 parent)
│   └── :day1:section03-wal         (section03-wal subproject)
└── (future day2 projects)
```

## Adding Day 2 Sections

1. Create your day2 section directory:
   ```bash
   mkdir -p day2/section01-mysection/src/main/java
   ```

2. Create `day2/section01-mysection/build.gradle`:
   ```groovy
   // Section-specific configuration
   dependencies {
       // Add any section-specific dependencies
       // Common dependencies are inherited
   }
   ```

3. Add to `settings.gradle` at the root:
   ```groovy
   include 'day2:section01-mysection'
   ```

4. Build it:
   ```bash
   ./gradlew :day2:section01-mysection:build
   ```

## Tips

- ✅ Always use the root `./gradlew` wrapper
- ✅ Use `:` notation for nested projects (e.g., `:day1:section03-wal`)
- ✅ Common dependencies are automatically inherited
- ✅ You can work on individual sections without building everything

## Current Structure

- **Root level**: Common configuration and dependencies
  - `day1/section03-wal`: Write-Ahead Log implementation
  - (Future sections to be added)

