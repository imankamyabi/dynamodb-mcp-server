#!/usr/bin/env node
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
  Tool,
} from "@modelcontextprotocol/sdk/types.js";
import {
  DynamoDBClient,
  CreateTableCommand,
  ListTablesCommand,
  DescribeTableCommand,
  UpdateTableCommand,
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  QueryCommand,
  ScanCommand,
} from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

// AWS client initialization
const credentials: { 
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
} = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID!,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY!,
};

if (process.env.AWS_SESSION_TOKEN) {
  credentials.sessionToken = process.env.AWS_SESSION_TOKEN;
}

const dynamoClient = new DynamoDBClient({
  region: process.env.AWS_REGION,
  credentials,
});

// Define tools
const CREATE_TABLE_TOOL: Tool = {
  name: "create_table",
  description: "Creates a new DynamoDB table with specified configuration",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table to create" },
      partitionKey: { type: "string", description: "Name of the partition key" },
      partitionKeyType: { type: "string", enum: ["S", "N", "B"], description: "Type of partition key (S=String, N=Number, B=Binary)" },
      sortKey: { type: "string", description: "Name of the sort key (optional)" },
      sortKeyType: { type: "string", enum: ["S", "N", "B"], description: "Type of sort key (optional)" },
      readCapacity: { type: "number", description: "Provisioned read capacity units" },
      writeCapacity: { type: "number", description: "Provisioned write capacity units" },
    },
    required: ["tableName", "partitionKey", "partitionKeyType", "readCapacity", "writeCapacity"],
  },
};

const LIST_TABLES_TOOL: Tool = {
  name: "list_tables",
  description: "Lists all DynamoDB tables in the account",
  inputSchema: {
    type: "object",
    properties: {
      limit: { type: "number", description: "Maximum number of tables to return (optional)" },
      exclusiveStartTableName: { type: "string", description: "Name of the table to start from for pagination (optional)" },
    },
  },
};

const CREATE_GSI_TOOL: Tool = {
  name: "create_gsi",
  description: "Creates a global secondary index on a table",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      indexName: { type: "string", description: "Name of the new index" },
      partitionKey: { type: "string", description: "Partition key for the index" },
      partitionKeyType: { type: "string", enum: ["S", "N", "B"], description: "Type of partition key" },
      sortKey: { type: "string", description: "Sort key for the index (optional)" },
      sortKeyType: { type: "string", enum: ["S", "N", "B"], description: "Type of sort key (optional)" },
      projectionType: { type: "string", enum: ["ALL", "KEYS_ONLY", "INCLUDE"], description: "Type of projection" },
      nonKeyAttributes: { type: "array", items: { type: "string" }, description: "Non-key attributes to project (optional)" },
      readCapacity: { type: "number", description: "Provisioned read capacity units" },
      writeCapacity: { type: "number", description: "Provisioned write capacity units" },
    },
    required: ["tableName", "indexName", "partitionKey", "partitionKeyType", "projectionType", "readCapacity", "writeCapacity"],
  },
};

const UPDATE_GSI_TOOL: Tool = {
  name: "update_gsi",
  description: "Updates the provisioned capacity of a global secondary index",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      indexName: { type: "string", description: "Name of the index to update" },
      readCapacity: { type: "number", description: "New read capacity units" },
      writeCapacity: { type: "number", description: "New write capacity units" },
    },
    required: ["tableName", "indexName", "readCapacity", "writeCapacity"],
  },
};

const CREATE_LSI_TOOL: Tool = {
  name: "create_lsi",
  description: "Creates a local secondary index on a table (must be done during table creation)",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      indexName: { type: "string", description: "Name of the new index" },
      partitionKey: { type: "string", description: "Partition key for the table" },
      partitionKeyType: { type: "string", enum: ["S", "N", "B"], description: "Type of partition key" },
      sortKey: { type: "string", description: "Sort key for the index" },
      sortKeyType: { type: "string", enum: ["S", "N", "B"], description: "Type of sort key" },
      projectionType: { type: "string", enum: ["ALL", "KEYS_ONLY", "INCLUDE"], description: "Type of projection" },
      nonKeyAttributes: { type: "array", items: { type: "string" }, description: "Non-key attributes to project (optional)" },
      readCapacity: { type: "number", description: "Provisioned read capacity units (optional, default: 5)" },
      writeCapacity: { type: "number", description: "Provisioned write capacity units (optional, default: 5)" },
    },
    required: ["tableName", "indexName", "partitionKey", "partitionKeyType", "sortKey", "sortKeyType", "projectionType"],
  },
};

const UPDATE_ITEM_TOOL: Tool = {
  name: "update_item",
  description: "Updates specific attributes of an item in a table",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      key: { type: "object", description: "Primary key of the item to update" },
      updateExpression: { type: "string", description: "Update expression (e.g., 'SET #n = :name')" },
      expressionAttributeNames: { type: "object", description: "Attribute name mappings" },
      expressionAttributeValues: { type: "object", description: "Values for the update expression" },
      conditionExpression: { type: "string", description: "Condition for update (optional)" },
      returnValues: { type: "string", enum: ["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"], description: "What values to return" },
    },
    required: ["tableName", "key", "updateExpression", "expressionAttributeNames", "expressionAttributeValues"],
  },
};

const UPDATE_CAPACITY_TOOL: Tool = {
  name: "update_capacity",
  description: "Updates the provisioned capacity of a table",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      readCapacity: { type: "number", description: "New read capacity units" },
      writeCapacity: { type: "number", description: "New write capacity units" },
    },
    required: ["tableName", "readCapacity", "writeCapacity"],
  },
};

const PUT_ITEM_TOOL: Tool = {
  name: "put_item",
  description: "Inserts or replaces an item in a table",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      item: { type: "object", description: "Item to put into the table" },
    },
    required: ["tableName", "item"],
  },
};

const GET_ITEM_TOOL: Tool = {
  name: "get_item",
  description: "Retrieves an item from a table by its primary key",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      key: { type: "object", description: "Primary key of the item to retrieve" },
    },
    required: ["tableName", "key"],
  },
};

const QUERY_TABLE_TOOL: Tool = {
  name: "query_table",
  description: "Queries a table using key conditions and optional filters",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      keyConditionExpression: { type: "string", description: "Key condition expression" },
      expressionAttributeValues: { type: "object", description: "Values for the key condition expression" },
      expressionAttributeNames: { type: "object", description: "Attribute name mappings", optional: true },
      filterExpression: { type: "string", description: "Filter expression for results", optional: true },
      limit: { type: "number", description: "Maximum number of items to return", optional: true },
    },
    required: ["tableName", "keyConditionExpression", "expressionAttributeValues"],
  },
};

const SCAN_TABLE_TOOL: Tool = {
  name: "scan_table",
  description: "Scans an entire table with optional filters",
  inputSchema: {
    type: "object", 
    properties: {
      tableName: { type: "string", description: "Name of the table" },
      filterExpression: { type: "string", description: "Filter expression", optional: true },
      expressionAttributeValues: { type: "object", description: "Values for the filter expression", optional: true },
      expressionAttributeNames: { type: "object", description: "Attribute name mappings", optional: true },
      limit: { type: "number", description: "Maximum number of items to return", optional: true },
    },
    required: ["tableName"],
  },
};

const DESCRIBE_TABLE_TOOL: Tool = {
  name: "describe_table",
  description: "Gets detailed information about a DynamoDB table",
  inputSchema: {
    type: "object",
    properties: {
      tableName: { type: "string", description: "Name of the table to describe" },
    },
    required: ["tableName"],
  },
};

// Implementation functions
async function createTable(params: any) {
  try {
    const command = new CreateTableCommand({
      TableName: params.tableName,
      AttributeDefinitions: [
        { AttributeName: params.partitionKey, AttributeType: params.partitionKeyType },
        ...(params.sortKey ? [{ AttributeName: params.sortKey, AttributeType: params.sortKeyType }] : []),
      ],
      KeySchema: [
        { AttributeName: params.partitionKey, KeyType: "HASH" as const },
        ...(params.sortKey ? [{ AttributeName: params.sortKey, KeyType: "RANGE" as const }] : []),
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: params.readCapacity,
        WriteCapacityUnits: params.writeCapacity,
      },
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Table ${params.tableName} created successfully`,
      details: response.TableDescription,
    };
  } catch (error) {
    console.error("Error creating table:", error);
    return {
      success: false,
      message: `Failed to create table: ${error}`,
    };
  }
}

async function listTables(params: any) {
  try {
    const command = new ListTablesCommand({
      Limit: params.limit,
      ExclusiveStartTableName: params.exclusiveStartTableName,
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: "Tables listed successfully",
      tables: response.TableNames,
      lastEvaluatedTable: response.LastEvaluatedTableName,
    };
  } catch (error) {
    console.error("Error listing tables:", error);
    return {
      success: false,
      message: `Failed to list tables: ${error}`,
    };
  }
}

async function createGSI(params: any) {
  try {
    const command = new UpdateTableCommand({
      TableName: params.tableName,
      AttributeDefinitions: [
        { AttributeName: params.partitionKey, AttributeType: params.partitionKeyType },
        ...(params.sortKey ? [{ AttributeName: params.sortKey, AttributeType: params.sortKeyType }] : []),
      ],
      GlobalSecondaryIndexUpdates: [
        {
          Create: {
            IndexName: params.indexName,
            KeySchema: [
              { AttributeName: params.partitionKey, KeyType: "HASH" as const },
              ...(params.sortKey ? [{ AttributeName: params.sortKey, KeyType: "RANGE" as const }] : []),
            ],
            Projection: {
              ProjectionType: params.projectionType,
              ...(params.projectionType === "INCLUDE" ? { NonKeyAttributes: params.nonKeyAttributes } : {}),
            },
            ProvisionedThroughput: {
              ReadCapacityUnits: params.readCapacity,
              WriteCapacityUnits: params.writeCapacity,
            },
          },
        },
      ],
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `GSI ${params.indexName} creation initiated on table ${params.tableName}`,
      details: response.TableDescription,
    };
  } catch (error) {
    console.error("Error creating GSI:", error);
    return {
      success: false,
      message: `Failed to create GSI: ${error}`,
    };
  }
}

async function updateGSI(params: any) {
  try {
    const command = new UpdateTableCommand({
      TableName: params.tableName,
      GlobalSecondaryIndexUpdates: [
        {
          Update: {
            IndexName: params.indexName,
            ProvisionedThroughput: {
              ReadCapacityUnits: params.readCapacity,
              WriteCapacityUnits: params.writeCapacity,
            },
          },
        },
      ],
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `GSI ${params.indexName} capacity updated on table ${params.tableName}`,
      details: response.TableDescription,
    };
  } catch (error) {
    console.error("Error updating GSI:", error);
    return {
      success: false,
      message: `Failed to update GSI: ${error}`,
    };
  }
}

async function createLSI(params: any) {
  try {
    // Note: LSIs must be created during table creation, so we need the table's primary key info
    const command = new CreateTableCommand({
      TableName: params.tableName,
      AttributeDefinitions: [
        { AttributeName: params.partitionKey, AttributeType: params.partitionKeyType },
        { AttributeName: params.sortKey, AttributeType: params.sortKeyType },
      ],
      KeySchema: [
        { AttributeName: params.partitionKey, KeyType: "HASH" as const },
      ],
      LocalSecondaryIndexes: [
        {
          IndexName: params.indexName,
          KeySchema: [
            { AttributeName: params.partitionKey, KeyType: "HASH" as const },
            { AttributeName: params.sortKey, KeyType: "RANGE" as const },
          ],
          Projection: {
            ProjectionType: params.projectionType,
            ...(params.projectionType === "INCLUDE" ? { NonKeyAttributes: params.nonKeyAttributes } : {}),
          },
        },
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: params.readCapacity || 5,
        WriteCapacityUnits: params.writeCapacity || 5,
      },
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `LSI ${params.indexName} created on table ${params.tableName}`,
      details: response.TableDescription,
    };
  } catch (error) {
    console.error("Error creating LSI:", error);
    return {
      success: false,
      message: `Failed to create LSI: ${error}`,
    };
  }
}

async function updateItem(params: any) {
  try {
    const command = new UpdateItemCommand({
      TableName: params.tableName,
      Key: marshall(params.key),
      UpdateExpression: params.updateExpression,
      ExpressionAttributeNames: params.expressionAttributeNames,
      ExpressionAttributeValues: marshall(params.expressionAttributeValues),
      ConditionExpression: params.conditionExpression,
      ReturnValues: params.returnValues || "NONE",
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Item updated successfully in table ${params.tableName}`,
      attributes: response.Attributes ? unmarshall(response.Attributes) : null,
    };
  } catch (error) {
    console.error("Error updating item:", error);
    return {
      success: false,
      message: `Failed to update item: ${error}`,
    };
  }
}

async function updateCapacity(params: any) {
  try {
    const command = new UpdateTableCommand({
      TableName: params.tableName,
      ProvisionedThroughput: {
        ReadCapacityUnits: params.readCapacity,
        WriteCapacityUnits: params.writeCapacity,
      },
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Capacity updated successfully for table ${params.tableName}`,
      details: response.TableDescription,
    };
  } catch (error) {
    console.error("Error updating capacity:", error);
    return {
      success: false,
      message: `Failed to update capacity: ${error}`,
    };
  }
}

async function putItem(params: any) {
  try {
    const command = new PutItemCommand({
      TableName: params.tableName,
      Item: marshall(params.item),
    });
    
    await dynamoClient.send(command);
    return {
      success: true,
      message: `Item added successfully to table ${params.tableName}`,
      item: params.item,
    };
  } catch (error) {
    console.error("Error putting item:", error);
    return {
      success: false,
      message: `Failed to put item: ${error}`,
    };
  }
}

async function getItem(params: any) {
  try {
    const command = new GetItemCommand({
      TableName: params.tableName,
      Key: marshall(params.key),
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Item retrieved successfully from table ${params.tableName}`,
      item: response.Item ? unmarshall(response.Item) : null,
    };
  } catch (error) {
    console.error("Error getting item:", error);
    return {
      success: false,
      message: `Failed to get item: ${error}`,
    };
  }
}

async function queryTable(params: any) {
  try {
    const command = new QueryCommand({
      TableName: params.tableName,
      KeyConditionExpression: params.keyConditionExpression,
      ExpressionAttributeValues: marshall(params.expressionAttributeValues),
      ExpressionAttributeNames: params.expressionAttributeNames,
      FilterExpression: params.filterExpression,
      Limit: params.limit,
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Query executed successfully on table ${params.tableName}`,
      items: response.Items ? response.Items.map(item => unmarshall(item)) : [],
      count: response.Count,
      scannedCount: response.ScannedCount,
    };
  } catch (error) {
    console.error("Error querying table:", error);
    return {
      success: false,
      message: `Failed to query table: ${error}`,
    };
  }
}

async function scanTable(params: any) {
  try {
    const command = new ScanCommand({
      TableName: params.tableName,
      FilterExpression: params.filterExpression,
      ExpressionAttributeValues: params.expressionAttributeValues ? marshall(params.expressionAttributeValues) : undefined,
      ExpressionAttributeNames: params.expressionAttributeNames,
      Limit: params.limit,
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Scan executed successfully on table ${params.tableName}`,
      items: response.Items ? response.Items.map(item => unmarshall(item)) : [],
      count: response.Count,
      scannedCount: response.ScannedCount,
    };
  } catch (error) {
    console.error("Error scanning table:", error);
    return {
      success: false,
      message: `Failed to scan table: ${error}`,
    };
  }
}

async function describeTable(params: any) {
  try {
    const command = new DescribeTableCommand({
      TableName: params.tableName,
    });
    
    const response = await dynamoClient.send(command);
    return {
      success: true,
      message: `Table ${params.tableName} described successfully`,
      table: response.Table,
    };
  } catch (error) {
    console.error("Error describing table:", error);
    return {
      success: false,
      message: `Failed to describe table: ${error}`,
    };
  }
}

// Server setup
const server = new Server(
  {
    name: "dynamodb-mcp-server",
    version: "0.1.0",
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

// Request handlers
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [CREATE_TABLE_TOOL, UPDATE_CAPACITY_TOOL, PUT_ITEM_TOOL, GET_ITEM_TOOL, QUERY_TABLE_TOOL, SCAN_TABLE_TOOL, DESCRIBE_TABLE_TOOL, LIST_TABLES_TOOL, CREATE_GSI_TOOL, UPDATE_GSI_TOOL, CREATE_LSI_TOOL, UPDATE_ITEM_TOOL],
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    let result;
    switch (name) {
      case "create_table":
        result = await createTable(args);
        break;
      case "list_tables":
        result = await listTables(args);
        break;
      case "create_gsi":
        result = await createGSI(args);
        break;
      case "update_gsi":
        result = await updateGSI(args);
        break;
      case "create_lsi":
        result = await createLSI(args);
        break;
      case "update_item":
        result = await updateItem(args);
        break;
      case "update_capacity":
        result = await updateCapacity(args);
        break;
      case "put_item":
        result = await putItem(args);
        break;
      case "get_item":
        result = await getItem(args);
        break;
      case "query_table":
        result = await queryTable(args);
        break;
      case "scan_table":
        result = await scanTable(args);
        break;
      case "describe_table":
        result = await describeTable(args);
        break;
      default:
        return {
          content: [{ type: "text", text: `Unknown tool: ${name}` }],
          isError: true,
        };
    }

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
    };
  } catch (error) {
    return {
      content: [{ type: "text", text: `Error occurred: ${error}` }],
      isError: true,
    };
  }
});

// Server startup
async function runServer() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("DynamoDB Server running on stdio");
}

runServer().catch((error) => {
  console.error("Fatal error running server:", error);
  process.exit(1);
});
