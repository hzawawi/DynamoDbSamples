using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace LearnDynamo
{
    public class DynamoClient
    {
        private readonly AmazonDynamoDBClient _amazonDynamoDBClient;
        private readonly DynamoDBContext _context;

        public DynamoClient()
        {
            //localstack ignores secrets
            _amazonDynamoDBClient = new AmazonDynamoDBClient("awsAccessKeyId", "awsSecretAccessKey",
                new AmazonDynamoDBConfig
                {
                    ServiceURL = "http://localhost:4569", //default localstack url
                    UseHttp = true,
                });

            _context = new DynamoDBContext(_amazonDynamoDBClient, new DynamoDBContextConfig
            {
                TableNamePrefix = "test_"
            });
        }

        public async Task<CreateTableResponse> SetupAsync()
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = "test_student",
                AttributeDefinitions = new List<AttributeDefinition>(),
                KeySchema = new List<KeySchemaElement>(),
                GlobalSecondaryIndexes = new List<GlobalSecondaryIndex>(),
                LocalSecondaryIndexes = new List<LocalSecondaryIndex>(),
                ProvisionedThroughput = new ProvisionedThroughput
                {
                    ReadCapacityUnits = 1,
                    WriteCapacityUnits = 1
                }
            };
            createTableRequest.KeySchema = new[]
            {
                new KeySchemaElement
                {
                    AttributeName = "Id",
                    KeyType = KeyType.HASH,
                },
            }.ToList();

            createTableRequest.AttributeDefinitions = new[]
            {
                new AttributeDefinition
                {
                    AttributeName = "Id",
                    AttributeType = ScalarAttributeType.N,
                }
            }.ToList();

            return await _amazonDynamoDBClient.CreateTableAsync(createTableRequest);
        }

        public async Task SaveOrUpdateStudent(Student student)
        {
            await _context.SaveAsync(student);
        }


        public async Task SaveOnlyStudent(Student student)
        {
            var identityEventTable = Table.LoadTable(_amazonDynamoDBClient, "test_student");

            var expression = new Expression
            {
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    {"#key", nameof(student.Id)},
                },
                ExpressionAttributeValues =
                {
                    {":key", student.Id},
                },
                ExpressionStatement = "attribute_not_exists(#key) OR #key <> :key",
            };

            var document = _context.ToDocument(student);

            await identityEventTable.PutItemAsync(document, new PutItemOperationConfig
            {
                ConditionalExpression = expression,
                ReturnValues = ReturnValues.None
            });
        }

        public async Task<Student> GetStudentUsingHashKey(int id)
        {
            return await _context.LoadAsync<Student>(id);
        }

        public async Task<Student> ScanForStudentUsingFirstName(string firstName)
        {
            var search = _context.ScanAsync<Student>
            (
                new[]
                {
                    new ScanCondition
                    (
                        nameof(Student.FirstName),
                        ScanOperator.Equal,
                        firstName
                    )
                }
            );
            var result = await search.GetRemainingAsync();
            return result.FirstOrDefault();
        }
    }
}