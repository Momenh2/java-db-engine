# DBEngine - Java Database Management System

A lightweight, file-based database management system implemented in Java with B+ tree indexing, page-based storage, and SQL-like query capabilities.

## 🚀 Features

### Core Database Operations
- **Table Creation**: Create tables with custom column types and clustering keys
- **Data Insertion**: Insert records with automatic page management
- **Data Updates**: Update existing records with clustering key-based lookups
- **Data Deletion**: Delete records with support for multiple conditions
- **Complex Queries**: SQL-like SELECT operations with AND, OR, XOR operators

### Advanced Features
- **B+ Tree Indexing**: High-performance indexing for fast data retrieval
- **Page-Based Storage**: Efficient memory management with configurable page sizes
- **Clustering Keys**: Primary key-based data organization
- **Metadata Management**: Automatic schema tracking and persistence
- **Serialization**: Persistent storage using Java serialization

### Query Capabilities
- **Comparison Operators**: `=`, `!=`, `<`, `>`, `<=`, `>=`
- **Logical Operators**: AND, OR, XOR for complex conditions
- **Indexed Queries**: Fast lookups using B+ tree indexes
- **Range Queries**: Efficient range-based data retrieval

## 🏗️ Architecture

### Core Components

#### 1. **DBApp** - Main Database Engine
- Central controller for all database operations
- Manages table creation, data manipulation, and query execution
- Handles metadata management and file I/O operations

#### 2. **Table** - Table Management
- Represents database tables with column definitions
- Manages page allocation and file naming
- Handles table serialization and persistence

#### 3. **Page** - Storage Unit
- Configurable page size for optimal memory usage
- Automatic page splitting and merging
- Tuple storage and retrieval within pages

#### 4. **Tuple** - Data Records
- Flexible data structure supporting multiple data types
- Automatic type conversion and validation
- Index update capabilities

#### 5. **B+ Tree** - Indexing Engine
- Self-balancing tree structure for efficient lookups
- Support for both integer and string keys
- Automatic tree rebalancing and node splitting

#### 6. **SQLTerm** - Query Building
- SQL-like query term representation
- Support for complex query conditions
- Operator and value management

### Storage Structure
```
dbengine/
├── src/
│   ├── DBApp.java          # Main database engine
│   ├── Table.java          # Table management
│   ├── Page.java           # Page storage
│   ├── Tuple.java          # Data records
│   ├── bplustree.java      # B+ tree indexing
│   ├── SQLTerm.java        # Query terms
│   ├── DBAppException.java # Exception handling
│   └── DbappConfig.java    # Configuration management
├── metadata.csv            # Schema and index metadata
├── *.class                 # Serialized table and index files
└── README.md              # This file
```

## 📦 Installation & Setup

### Prerequisites
- Java 8 or higher
- Any Java IDE (Eclipse, IntelliJ IDEA, VS Code)

### Quick Start
1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/dbengine.git
   cd dbengine
   ```

2. **Compile the project**
   ```bash
   javac -d bin src/*.java
   ```

3. **Run the database engine**
   ```bash
   java -cp bin DBApp
   ```

## 💻 Usage Examples

### 1. Creating a Table
```java
DBApp dbApp = new DBApp();

// Define column types
Hashtable<String, String> htblColNameType = new Hashtable<>();
htblColNameType.put("id", "java.lang.Integer");
htblColNameType.put("name", "java.lang.String");
htblColNameType.put("gpa", "java.lang.Double");

// Create table with clustering key
dbApp.createTable("Student", "gpa", htblColNameType);
```

### 2. Inserting Data
```java
// Insert a student record
Hashtable<String, Object> htblColNameValue = new Hashtable<>();
htblColNameValue.put("id", new Integer(12345));
htblColNameValue.put("name", "John Doe");
htblColNameValue.put("gpa", new Double(3.8));

dbApp.insertIntoTable("Student", htblColNameValue);
```

### 3. Creating Indexes
```java
// Create B+ tree indexes for fast lookups
dbApp.createIndex("Student", "id", "idIndex");
dbApp.createIndex("Student", "name", "nameIndex");
dbApp.createIndex("Student", "gpa", "gpaIndex");
```

### 4. Complex Queries
```java
// Create SQL terms for query conditions
SQLTerm[] arrSQLTerms = new SQLTerm[2];

arrSQLTerms[0] = new SQLTerm();
arrSQLTerms[0]._strTableName = "Student";
arrSQLTerms[0]._strColumnName = "gpa";
arrSQLTerms[0]._strOperator = ">=";
arrSQLTerms[0]._objValue = "3.5";

arrSQLTerms[1] = new SQLTerm();
arrSQLTerms[1]._strTableName = "Student";
arrSQLTerms[1]._strColumnName = "name";
arrSQLTerms[1]._strOperator = "=";
arrSQLTerms[1]._objValue = "John Doe";

// Define logical operators
String[] strarrOperators = new String[1];
strarrOperators[0] = "AND";

// Execute query
Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
while(resultSet.hasNext()) {
    System.out.println(resultSet.next());
}
```

### 5. Updating Data
```java
// Update student record
Hashtable<String, Object> htblColNameValue = new Hashtable<>();
htblColNameValue.put("name", "Jane Doe");
htblColNameValue.put("gpa", new Double(3.9));

dbApp.updateTable("Student", "3.8", htblColNameValue);
```

### 6. Deleting Data
```java
// Delete records matching conditions
Hashtable<String, Object> htblColNameValue = new Hashtable<>();
htblColNameValue.put("gpa", new Double(2.0));

dbApp.deleteFromTable("Student", htblColNameValue);
```

## 🔧 Configuration

### Page Size Configuration
The database engine uses a configuration file to manage page sizes and other settings:

```java
// Default configuration
DbappConfig config = new DbappConfig("path/to/config/");
// Maximum rows per page: 200 (default)
```

### Metadata Management
The system automatically maintains a `metadata.csv` file containing:
- Table schemas
- Column definitions
- Index information
- Data types and constraints

## 🚀 Performance Features

### B+ Tree Indexing
- **Fast Lookups**: O(log n) search complexity
- **Range Queries**: Efficient range-based operations
- **Automatic Balancing**: Self-balancing tree structure
- **Memory Efficient**: Optimized for disk-based storage

### Page-Based Storage
- **Configurable Page Size**: Adjustable based on memory constraints
- **Automatic Splitting**: Pages split when full
- **Efficient I/O**: Minimized disk access through page caching
- **Memory Management**: Optimal memory usage for large datasets

### Query Optimization
- **Index Usage**: Automatic index selection for queries
- **Condition Optimization**: Efficient filtering and sorting
- **Logical Operations**: Optimized AND, OR, XOR operations

## 🛠️ Development

### Project Structure
```
src/
├── DBApp.java              # Main database engine (2239 lines)
├── Table.java              # Table management (193 lines)
├── Page.java               # Page storage (224 lines)
├── Tuple.java              # Data records (254 lines)
├── bplustree.java          # B+ tree implementation (1414 lines)
├── SQLTerm.java            # Query terms (13 lines)
├── DBAppException.java     # Custom exceptions
├── DbappConfig.java        # Configuration management
└── Serializer.java         # Serialization utilities
```

### Key Classes

#### DBApp
- **Main Engine**: Central database controller
- **CRUD Operations**: Create, Read, Update, Delete
- **Query Processing**: SQL-like query execution
- **Index Management**: B+ tree creation and maintenance

#### B+ Tree Implementation
- **Internal Nodes**: Non-leaf tree nodes
- **Leaf Nodes**: Data storage nodes
- **Dictionary Pairs**: Key-value storage
- **Tree Balancing**: Automatic rebalancing algorithms

#### Page Management
- **Tuple Storage**: Efficient data organization
- **Page Splitting**: Automatic page management
- **Serialization**: Persistent storage
- **Memory Optimization**: Configurable page sizes

## 🧪 Testing

### Running Tests
```java
// Example test case
public class DBAppTest {
    public static void main(String[] args) {
        DBApp dbApp = new DBApp();
        
        // Test table creation
        // Test data insertion
        // Test query execution
        // Test index operations
    }
}
```

### Sample Test Data
The project includes sample data and operations in the main method for testing:
- Student table with ID, Name, GPA columns
- Multiple index creation examples
- Complex query demonstrations
- Update and delete operations

## 🤝 Contributing

### Development Guidelines
1. **Code Style**: Follow Java naming conventions
2. **Documentation**: Add comments for complex algorithms
3. **Testing**: Include test cases for new features
4. **Performance**: Optimize for large datasets

### Areas for Enhancement
- **SQL Parser**: Full SQL syntax support
- **Transaction Management**: ACID compliance
- **Concurrency Control**: Multi-user support
- **Query Optimization**: Advanced query planning
- **Backup/Recovery**: Data protection features

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **B+ Tree Implementation**: Based on standard B+ tree algorithms
- **Page-Based Storage**: Inspired by database management systems
- **Java Serialization**: Leveraging Java's built-in persistence

## 📞 Support

For questions, issues, or contributions:
- **Issues**: Use GitHub issues for bug reports
- **Discussions**: Use GitHub discussions for questions
- **Contributions**: Pull requests are welcome

---

**DBEngine** - A powerful, lightweight database management system built in Java with advanced indexing and query capabilities. 