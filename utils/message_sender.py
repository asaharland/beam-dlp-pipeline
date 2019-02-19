from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('red-dog-piano', 'pii-messages')

messages = ['{"id": 1,"text": "My credit card number is 3765-414016-21817."}',
            '{"id": 2,"text": "My number should be 0333 7591 018."}']

for n in range(0, 10):
    for message in messages:
        data = message.encode('utf-8')
        # When you publish a message, the client returns a Future.
        message_future = publisher.publish(topic_path, data=data)
