
**AMPS Connector for Acunu Analytics**

Copyright (c) 2013 Acunu. See LICENSE for license terms.

Requires Acunu Analytics >=v5.0 and the AMPS client jar.

AMPS is a real-time streaming messaging system developed by 60East 
Technologies, Inc. This integration allows you to use the AMPS Java 
client to connect to an AMPS server, subscribe to topics and 
receive events on these topics in Acunu Analytics.

Download a copy of the [AMPS Java client](http://www.crankuptheamps.com/documentation/client-apis/java/) 
and place the `amps_client.jar` file in the connector's `lib` sub-directory.

To build the connector, first check `$JBIRD_HOME` points to your Acunu 
Analytics installation:

    export JBIRD_HOME=/usr/share/acunu/jbird/

You'll need the JARs that are supplied with Analytics, especially the 
Analytics Connectors API, `analytics-connectors.jar`.
  
To build the connector, run:

    make
  
And to install the connector into the `$JBIRD_HOME/plugins` directory:

    sudo make install
  
Or to install it somewhere else:

    PLUGINS_DIR=/my/path/to/plugins/ make install
  
You'll need to restart Analytics for it to pick up the new connector.

Then, create an AMPS ingester:

    CREATE INGESTER amps USING 'com.acunu.analytics.amps.AmpsIngester' 
      PROPERTIES url = 'tcp://ampshost:9004/nvfix';

The url parameter is mandatory, it points to the message queue server.

And create a flow that uses it (having created a table first):

    CREATE FLOW my_flow INGESTER amps RECEIVER my_table 
      PROPERTIES topic = 'topic1';

This uses the default decoder, which expects Strings, bytes or chars to 
be sent, and will interpret JSON objects. If you're sending messages 
in a particular format, can specify your own decoder:

    CREATE FLOW my_flow INGESTER amps DECODER 'com.my.company.MyDecoder' 
      RECEIVER my_table PROPERTIES topic = 'topic1';

See the [Acunu documentation](http://www.acunu.com/documentation.html#%2Fv5.0%2Fdeveloper%2Fplugins.html) 
for more details about decoders. 

Once you've set up a flow, test it with an AMPS client that sends messages
over the bus. Start the AMPS server, then start a client to send messages
on the same topic as you configured in the ingester and flow.

We welcome questions, suggestions, feedback, patches and problems reports. 

Please get in touch at http://support.acunu.com/

