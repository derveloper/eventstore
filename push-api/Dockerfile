FROM vertx/vertx3

ENV VERTICLE_NAME eventstore.boundary.PushApi
ENV VERTICLE_FILE target/*fat*.jar

# Set the location of the verticles
ENV VERTICLE_HOME /usr/verticles

# Copy your verticle to the container
COPY $VERTICLE_FILE $VERTICLE_HOME/

# Launch the verticle
WORKDIR $VERTICLE_HOME
ENTRYPOINT ["sh", "-c"]
CMD ["vertx run $VERTICLE_NAME -cp $VERTICLE_HOME/*  -cluster -ha"]
