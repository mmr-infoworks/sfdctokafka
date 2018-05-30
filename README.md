1. Create a topic/channel
  This is done for accounts in following manner

the query and topic name needs to change for each entity

PushTopic pushTopic = new PushTopic();
pushTopic.Name = 'AccountInserts';
pushTopic.Query = 'select Id, name,type from account';   
pushTopic.ApiVersion = 37.0;
pushTopic.NotifyForOperationCreate = true;
pushTopic.NotifyForOperationUpdate = true;
pushTopic.NotifyForOperationUndelete = true;
pushTopic.NotifyForOperationDelete = true;
pushTopic.NotifyForFields = 'Referenced';
insert pushTopic;

run this code ONCE in 
https://na50.salesforce.com/_ui/common/apex/debug/ApexCSIPage
Debug --> Execute Anonymous window

This will create the channel topic 

Reference : https://developer.salesforce.com/docs/atlas.en-us.api_streaming.meta/api_streaming/code_sample_java_create_pushtopic.htm

2. Run LoginExample.java with parameters ( Have kafka running)
jatin@Infoworks.io IN11**rkD0nAzNh32noa6Emjn6vcq2jP /topic/AccountInserts

only topicname will change according to entity
