# DynamoDB MCP Server

A [Model Context Protocol server](https://modelcontextprotocol.io/) for managing Amazon DynamoDB resources. This server provides tools for table management, capacity management, and data operations.

## Author

Iman Kamyabi (ikmyb@icloud.com)

## Features

### Table Management
- Create new DynamoDB tables with customizable configurations
- List existing tables
- Get detailed table information
- Configure table settings

### Index Management
- Create and manage Global Secondary Indexes (GSI)
- Update GSI capacity
- Create Local Secondary Indexes (LSI)

### Capacity Management
- Update provisioned read/write capacity units
- Manage table throughput settings

### Data Operations
- Insert or replace items in tables
- Retrieve items by primary key
- Update specific item attributes
- Query tables with conditions
- Scan tables with filters

> **Note**: Delete operations are not supported to prevent accidental data loss.

## Setup

1. Install dependencies:
```bash
npm install
```

2. Configure AWS credentials as environment variables:
```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_REGION="your_region"
```

3. Build the server:
```bash
npm run build
```

4. Start the server:
```bash
npm start
```

## Tools

### create_table
Creates a new DynamoDB table with specified configuration.

Parameters:
- `tableName`: Name of the table to create
- `partitionKey`: Name of the partition key
- `partitionKeyType`: Type of partition key (S=String, N=Number, B=Binary)
- `sortKey`: (Optional) Name of the sort key
- `sortKeyType`: (Optional) Type of sort key
- `readCapacity`: Provisioned read capacity units
- `writeCapacity`: Provisioned write capacity units

Example:
```json
{
  "tableName": "Users",
  "partitionKey": "userId",
  "partitionKeyType": "S",
  "readCapacity": 5,
  "writeCapacity": 5
}
```

### list_tables
Lists all DynamoDB tables in the account.

Parameters:
- `limit`: (Optional) Maximum number of tables to return
- `exclusiveStartTableName`: (Optional) Name of the table to start from for pagination

Example:
```json
{
  "limit": 10
}
```

### describe_table
Gets detailed information about a DynamoDB table.

Parameters:
- `tableName`: Name of the table to describe

Example:
```json
{
  "tableName": "Users"
}
```

### create_gsi
Creates a global secondary index on a table.

Parameters:
- `tableName`: Name of the table
- `indexName`: Name of the new index
- `partitionKey`: Partition key for the index
- `partitionKeyType`: Type of partition key
- `sortKey`: (Optional) Sort key for the index
- `sortKeyType`: (Optional) Type of sort key
- `projectionType`: Type of projection (ALL, KEYS_ONLY, INCLUDE)
- `nonKeyAttributes`: (Optional) Non-key attributes to project
- `readCapacity`: Provisioned read capacity units
- `writeCapacity`: Provisioned write capacity units

Example:
```json
{
  "tableName": "Users",
  "indexName": "EmailIndex",
  "partitionKey": "email",
  "partitionKeyType": "S",
  "projectionType": "ALL",
  "readCapacity": 5,
  "writeCapacity": 5
}
```

### update_gsi
Updates the provisioned capacity of a global secondary index.

Parameters:
- `tableName`: Name of the table
- `indexName`: Name of the index to update
- `readCapacity`: New read capacity units
- `writeCapacity`: New write capacity units

Example:
```json
{
  "tableName": "Users",
  "indexName": "EmailIndex",
  "readCapacity": 10,
  "writeCapacity": 10
}
```

### create_lsi
Creates a local secondary index on a table (must be done during table creation).

Parameters:
- `tableName`: Name of the table
- `indexName`: Name of the new index
- `partitionKey`: Partition key for the table
- `partitionKeyType`: Type of partition key
- `sortKey`: Sort key for the index
- `sortKeyType`: Type of sort key
- `projectionType`: Type of projection (ALL, KEYS_ONLY, INCLUDE)
- `nonKeyAttributes`: (Optional) Non-key attributes to project
- `readCapacity`: (Optional) Provisioned read capacity units
- `writeCapacity`: (Optional) Provisioned write capacity units

Example:
```json
{
  "tableName": "Users",
  "indexName": "CreatedAtIndex",
  "partitionKey": "userId",
  "partitionKeyType": "S",
  "sortKey": "createdAt",
  "sortKeyType": "N",
  "projectionType": "ALL"
}
```

### update_capacity
Updates the provisioned capacity of a table.

Parameters:
- `tableName`: Name of the table
- `readCapacity`: New read capacity units
- `writeCapacity`: New write capacity units

Example:
```json
{
  "tableName": "Users",
  "readCapacity": 10,
  "writeCapacity": 10
}
```

### put_item
Inserts or replaces an item in a table.

Parameters:
- `tableName`: Name of the table
- `item`: Item to put into the table (as JSON object)

Example:
```json
{
  "tableName": "Users",
  "item": {
    "userId": "123",
    "name": "John Doe",
    "email": "john@example.com"
  }
}
```

### get_item
Retrieves an item from a table by its primary key.

Parameters:
- `tableName`: Name of the table
- `key`: Primary key of the item to retrieve

Example:
```json
{
  "tableName": "Users",
  "key": {
    "userId": "123"
  }
}
```

### update_item
Updates specific attributes of an item in a table.

Parameters:
- `tableName`: Name of the table
- `key`: Primary key of the item to update
- `updateExpression`: Update expression
- `expressionAttributeNames`: Attribute name mappings
- `expressionAttributeValues`: Values for the update expression
- `conditionExpression`: (Optional) Condition for update
- `returnValues`: (Optional) What values to return

Example:
```json
{
  "tableName": "Users",
  "key": {
    "userId": "123"
  },
  "updateExpression": "SET #n = :name",
  "expressionAttributeNames": {
    "#n": "name"
  },
  "expressionAttributeValues": {
    ":name": "Jane Doe"
  }
}
```

### query_table
Queries a table using key conditions and optional filters.

Parameters:
- `tableName`: Name of the table
- `keyConditionExpression`: Key condition expression
- `expressionAttributeValues`: Values for the key condition expression
- `expressionAttributeNames`: (Optional) Attribute name mappings
- `filterExpression`: (Optional) Filter expression for results
- `limit`: (Optional) Maximum number of items to return

Example:
```json
{
  "tableName": "Users",
  "keyConditionExpression": "userId = :id",
  "expressionAttributeValues": {
    ":id": "123"
  }
}
```

### scan_table
Scans an entire table with optional filters.

Parameters:
- `tableName`: Name of the table
- `filterExpression`: (Optional) Filter expression
- `expressionAttributeValues`: (Optional) Values for the filter expression
- `expressionAttributeNames`: (Optional) Attribute name mappings
- `limit`: (Optional) Maximum number of items to return

Example:
```json
{
  "tableName": "Users",
  "filterExpression": "age > :minAge",
  "expressionAttributeValues": {
    ":minAge": 21
  }
}
```

## Sample Questions

Here are some example questions you can ask Claude when using this DynamoDB MCP server:

### Table Management
- "Create a new DynamoDB table called 'Products' with a partition key 'productId' (string) and sort key 'timestamp' (number)"
- "List all DynamoDB tables in my account"
- "What's the current configuration of the Users table?"
- "Add a global secondary index on the email field of the Users table"

### Capacity Management
- "Update the Users table capacity to 20 read units and 15 write units"
- "Scale up the EmailIndex GSI capacity on the Users table"
- "What's the current provisioned capacity for the Orders table?"

### Data Operations
- "Insert a new user with ID '123', name 'John Doe', and email 'john@example.com'"
- "Get the user with ID '123'"
- "Update the email address for user '123' to 'john.doe@example.com'"
- "Find all orders placed by user '123'"
- "List all users who are over 21 years old"
- "Query the EmailIndex to find the user with email 'john@example.com'"

## Configuration

### Setting up AWS Credentials

1. Obtain AWS access key ID, secret access key, and region from the AWS Management Console.
2. If using temporary credentials (e.g., IAM role), also obtain a session token.
3. Ensure these credentials have appropriate permissions for DynamoDB operations.

### Usage with Claude Desktop

Add this to your `claude_desktop_config.json`:

#### Docker (Recommended)

```json
{
  "mcpServers": {
    "dynamodb": {
      "command": "docker",
      "args": [ "run", "-i", "--rm", "-e", "AWS_ACCESS_KEY_ID", "-e", "AWS_SECRET_ACCESS_KEY", "-e", "AWS_REGION", "-e", "AWS_SESSION_TOKEN", "mcp/dynamodb-mcp-server" ],
      "env": {
        "AWS_ACCESS_KEY_ID": "your_access_key",
        "AWS_SECRET_ACCESS_KEY": "your_secret_key",
        "AWS_REGION": "your_region",
        "AWS_SESSION_TOKEN": "your_session_token"  
      }
    }
  }
}
```

## Building

Docker:
```sh
docker build -t mcp/dynamodb-mcp-server -f Dockerfile .
```

## Development

To run in development mode with auto-reloading:
```bash
npm run dev
```

## License

This MCP server is licensed under the MIT License. This means you are free to use, modify, and distribute the software, subject to the terms and conditions of the MIT License. For more details, please see the LICENSE file in the project repository.
