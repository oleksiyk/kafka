FROM spotify/kafka

RUN echo $KAFKA_HOME
RUN echo "listeners=PLAINTEXT://:9092,SSL://:9093" >> $KAFKA_HOME/config/server.properties
RUN echo "ssl.keystore.location=$KAFKA_HOME/server.keystore.jks" >> $KAFKA_HOME/config/server.properties
RUN echo "ssl.keystore.password=test1234" >> $KAFKA_HOME/config/server.properties
RUN echo "ssl.key.password=test1234" >> $KAFKA_HOME/config/server.properties;
RUN echo "ssl.truststore.location=${KAFKA_HOME}/server.truststore.jks" >> $KAFKA_HOME/config/server.properties
RUN echo "ssl.truststore.password=test1234" >> $KAFKA_HOME/config/server.properties

ADD ssl/server.keystore.jks $KAFKA_HOME/server.keystore.jks
ADD ssl/server.truststore.jks $KAFKA_HOME/server.truststore.jks

EXPOSE 2181 9092 9093

CMD ["supervisord", "-n"]
