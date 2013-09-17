#################################
# CONNECTOR.SECURITY.PROPERTIES #
#################################

# Keystore and Trustore for SSL and encryption
security.keystore.location=@{JAVA_CONNECTOR_KEYSTORE_PATH}
security.keystore.password=@{JAVA_CONNECTOR_KEYSTORE_PASSWORD}
security.truststore.location=@{JAVA_CONNECTOR_TRUSTSTORE_PATH}
security.truststore.password=@{JAVA_CONNECTOR_TRUSTSTORE_PASSWORD}