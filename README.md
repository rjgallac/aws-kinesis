docker exec -it s3stack sh



awslocal dynamodb list-tables


awslocal kinesis create-stream --stream-name test-stream --shard-count 3

awslocal kinesis list-streams

awslocal dynamodb scan --table-name test-app

awslocal kinesis put-record --stream-name test-stream --data sampledatarecord --partition-key samplepartitionkey