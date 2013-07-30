#################################
# SECURITY.PROPERTIES           #
#################################

# Location of files used for security. 
security.dir=@{SECURITY_DIRECTORY}

# RMI + JMX authentication and encryption parameters
security.rmi.authentication=@{ENABLE_RMI_AUTHENTICATION}
security.rmi.tungsten.authenticationRealm=true
security.rmi.tungsten.authenticationRealm.encrypted.password=true
security.rmi.encryption=@{ENABLE_RMI_SSL}
security.rmi.username=@{RMI_USER}

# Password and access file
security.password_file.location=@{JAVA_PASSWORDSTORE_PATH}
security.access_file.location=@{JAVA_JMXREMOTE_ACCESS_PATH}

# Keystore and Trustore for SSL and encryption
security.keystore.location=@{JAVA_KEYSTORE_PATH}
security.keystore.password=@{JAVA_KEYSTORE_PASSWORD}
security.truststore.location=@{JAVA_TRUSTSTORE_PATH}
security.truststore.password=@{JAVA_TRUSTSTORE_PASSWORD}