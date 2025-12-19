# Testing Guide

## Overview

This project includes comprehensive unit tests for all transformation logic. Tests are written using JUnit 4 and follow Spark testing best practices.

## Test Structure

```
src/test/java/com/ecommerce/
├── test/
│   └── SparkTestBase.java              # Base class for Spark tests
└── transformation/
    └── silver/
        ├── OrdersSilverTransformerTest.java
        ├── CustomersSilverTransformerTest.java  (TODO)
        └── ProductsSilverTransformerTest.java   (TODO)
```

## Running Tests

### Run All Tests
```bash
mvn test
```

### Run Specific Test Class
```bash
mvn test -Dtest=OrdersSilverTransformerTest
```

### Run Specific Test Method
```bash
mvn test -Dtest=OrdersSilverTransformerTest#testDataQualityRules_ValidOrders_ShouldPass
```

### Run Tests with Verbose Output
```bash
mvn test -X
```

## Test Coverage

### OrdersSilverTransformer
- ✅ Data Quality Rules
  - Valid orders pass validation
  - Null order_id is quarantined
  - Invalid status is quarantined
  - Null customer_id is quarantined
  - Null purchase timestamp is quarantined
  - Illogical delivery dates are quarantined
  
- ✅ Deduplication Logic
  - Duplicate orders are deduplicated
  - Latest record is kept based on purchase timestamp
  
- ✅ Enrichment Logic
  - Delivery time calculation is correct
  - Late delivery flag is set correctly
  - Approval time calculation is correct
  - Date parts extraction (year, month, quarter) is correct

### CustomersSilverTransformer (TODO)
- Data quality validation
- Geographic region mapping
- City tier assignment
- Deduplication logic

### ProductsSilverTransformer (TODO)
- Physical dimension validation
- Volume calculations
- Weight categorization
- Shipping complexity scoring

## Writing New Tests

### 1. Extend SparkTestBase

```java
public class MyTransformerTest extends SparkTestBase {
    // Your tests here
}
```

### 2. Create Test Data

```java
private Dataset<Row> createTestData() {
    StructType schema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.StringType, false),
        DataTypes.createStructField("value", DataTypes.IntegerType, true)
    ));
    
    List<Row> data = Arrays.asList(
        RowFactory.create("1", 100),
        RowFactory.create("2", 200)
    );
    
    return spark.createDataFrame(data, schema);
}
```

### 3. Write Focused Tests

```java
@Test
public void testSpecificBehavior_GivenCondition_ShouldExpectResult() {
    // Arrange: Set up test data
    Dataset<Row> input = createTestData();
    
    // Act: Execute the transformation
    Dataset<Row> result = transformer.transform(input);
    
    // Assert: Verify the results
    assertEquals("Expected 2 records", 2, result.count());
}
```

## Test Naming Convention

We follow the pattern: `test<MethodName>_<Condition>_<ExpectedResult>`

Examples:
- `testDataQualityRules_ValidOrders_ShouldPass`
- `testDeduplication_DuplicateOrders_ShouldKeepLatest`
- `testEnrichment_LateDelivery_ShouldFlagCorrectly`

## Best Practices

### ✅ DO:
- Write one assertion per test when possible
- Use descriptive test names that explain what's being tested
- Create small, focused test datasets
- Test edge cases and error conditions
- Keep tests independent (no test depends on another)
- Clean up test data after tests run

### ❌ DON'T:
- Don't test Spark framework itself (it's already tested)
- Don't create huge test datasets (keep it small and focused)
- Don't use real production data in tests
- Don't ignore failing tests
- Don't test multiple things in one test method

## Debugging Failed Tests

### 1. Check Test Logs
```bash
mvn test -X > test-output.log 2>&1
```

### 2. Print DataFrame Contents
```java
@Test
public void debugTest() {
    Dataset<Row> result = transformer.transform(input);
    result.show(false);  // Show all columns without truncation
    result.printSchema();  // Print schema
}
```

### 3. Run Single Test in IDE
- Right-click on test method
- Select "Run" or "Debug"
- Set breakpoints as needed

## Continuous Integration

Tests run automatically on every commit via Maven:
```bash
mvn clean verify
```

This ensures:
- All tests pass
- Code compiles successfully
- No regressions introduced

## Test Metrics

Current test coverage:
- **OrdersSilverTransformer**: 7 tests ✅
- **CustomersSilverTransformer**: 0 tests (TODO)
- **ProductsSilverTransformer**: 0 tests (TODO)

Target: 80%+ code coverage for all transformers

## Performance Considerations

### SparkSession Reuse
- Tests share a single SparkSession (`local[2]`)
- Reduces test execution time significantly
- First test takes longer (SparkSession creation)
- Subsequent tests are much faster

### Typical Test Execution Times
- Single test: ~1-2 seconds
- Full test suite: ~10-15 seconds
- First run (with SparkSession creation): ~5-10 seconds longer

## Common Issues

### Issue: Tests timeout
**Solution**: Increase timeout in pom.xml
```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-surefire-plugin</artifactId>
    <configuration>
        <forkedProcessTimeoutInSeconds>300</forkedProcessTimeoutInSeconds>
    </configuration>
</plugin>
```

### Issue: Tests fail with "Port already in use"
**Solution**: SparkSession wasn't cleaned up properly. Restart IDE or kill Java processes.

### Issue: Tests pass locally but fail in CI
**Solution**: Check for:
- Hardcoded paths (use relative paths)
- Time-dependent logic (use fixed test data)
- Environment-specific configurations

## Next Steps

1. **Add more tests** for Customers and Products transformers
2. **Add integration tests** for end-to-end pipeline
3. **Add data quality monitoring tests**
4. **Set up test coverage reporting** (JaCoCo)
5. **Add performance benchmarks**

## Resources

- [JUnit 4 Documentation](https://junit.org/junit4/)
- [Apache Spark Testing Best Practices](https://spark.apache.org/docs/latest/sql-ref-functions.html)
- [Mockito Documentation](https://site.mockito.org/)

---

**Happy Testing! 🧪**