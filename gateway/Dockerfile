FROM java:8

ENV INSTALL_FILE_BASE iot-gateway-0.2.0-SNAPSHOT

COPY build/distributions/${INSTALL_FILE_BASE}.tar /tmp

RUN \
  mkdir -p /opt/gateway \
  && tar -xvf /tmp/${INSTALL_FILE_BASE}.tar -C /opt/gateway --strip 1

WORKDIR /opt/gateway

ENTRYPOINT [ "/opt/gateway/bin/gateway" ]
