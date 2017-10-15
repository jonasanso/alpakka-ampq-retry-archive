Just playing with Akka Streams and Alpakka AMQP connector.

Alpakka AMQP has a lovely API for declaring queues and combined with the power
of Akka Streams you have a wonderful tool to manage your async messages.

There is some boring code to handle the properties and some tricks to make the retry queue work.

#### Versions
Akka 2.5.6 and Alpakka 0.14

#### Run it
To try it out first launch a rabbit Mq and connect to it. Check config in example.Main

Run it and send messages with numbers to the example queue.

```sbt run ```

It creates the queues example, example.retry and example.archive

Negative numbers will be retried 3 times then archived.

Numbers above 10 will be handled the first time

Numbers between 0 and 10 will randomly be retried or handled,


#### TO DO
- Simplify using retry mechanisms in Akka and avoid the retry queue 