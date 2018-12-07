/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.requests;

import static org.apache.kafka.common.protocol.Protocol.REQUEST_HEADER;
import static org.apache.kafka.common.protocol.Protocol.REQUEST_SECURITY_HEADER;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.common.protocol.Protocol;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The header for a request in the Kafka protocol
 */
public class RequestHeader extends AbstractRequestResponse {

    private static final Logger log = LoggerFactory.getLogger(RequestHeader.class);

    private static final String VerifyClientVersionEnableProp = "verify.client.version.enable";
    private static boolean security = enableSecurity();

    private static final Field API_KEY_FIELD = getField("api_key");
    private static final Field API_VERSION_FIELD = getField("api_version");
    private static final Field CLIENT_ID_FIELD = getField("client_id");
    private static final Field CLIENT_VERSION_FIELD = getField("client_version");
    private static final Field CORRELATION_ID_FIELD = getField("correlation_id");

    private short apiKey;
    private short apiVersion;
    private String clientId;
    private String clientVersion;
    private int correlationId;

    public RequestHeader(Struct header) {
        super(header);
        if (security) {
            apiKey = struct.getShort(API_KEY_FIELD);
            apiVersion = struct.getShort(API_VERSION_FIELD);
            clientId = struct.getString(CLIENT_ID_FIELD);
            clientVersion = struct.getString(CLIENT_VERSION_FIELD);
            correlationId = struct.getInt(CORRELATION_ID_FIELD);
        } else {
            apiKey = struct.getShort(API_KEY_FIELD);
            apiVersion = struct.getShort(API_VERSION_FIELD);
            clientId = struct.getString(CLIENT_ID_FIELD);
            correlationId = struct.getInt(CORRELATION_ID_FIELD);
        }

    }

    public RequestHeader(short apiKey, short version, String client, int correlation) {
        super(setStruct());
        if (security) {
            struct.set(API_KEY_FIELD, apiKey);
            struct.set(API_VERSION_FIELD, version);
            struct.set(CLIENT_ID_FIELD, client);
            struct.set(CLIENT_VERSION_FIELD, "cmss-client");
            struct.set(CORRELATION_ID_FIELD, correlation);
            this.apiKey = apiKey;
            this.apiVersion = version;
            this.clientId = client;
            this.clientVersion = "cmss-client";
            this.correlationId = correlation;
        } else {
            struct.set(API_KEY_FIELD, apiKey);
            struct.set(API_VERSION_FIELD, version);
            struct.set(CLIENT_ID_FIELD, client);
            struct.set(CORRELATION_ID_FIELD, correlation);
            this.apiKey = apiKey;
            this.apiVersion = version;
            this.clientId = client;
            this.correlationId = correlation;
        }
    }

    public short apiKey() {
        return apiKey;
    }

    public short apiVersion() {
        return apiVersion;
    }

    public String clientId() {
        return clientId;
    }

    public String clientVersion() {
        return clientVersion;
    }

    public int correlationId() {
        return correlationId;
    }

    private static Struct setStruct() {
        if (security) {
            return new Struct(Protocol.REQUEST_SECURITY_HEADER);
        } else {
            return new Struct(Protocol.REQUEST_HEADER);
        }
    }

    public static Field getField(String field) {
        if (security) {
            return REQUEST_SECURITY_HEADER.get(field);
        } else {
            return REQUEST_HEADER.get(field);
        }
    }

    public static RequestHeader parse(ByteBuffer buffer) {
        if (security)
            return new RequestHeader(Protocol.REQUEST_SECURITY_HEADER.read(buffer));
        else
            return new RequestHeader(Protocol.REQUEST_HEADER.read(buffer));
    }

    public static boolean enableSecurity() {
        String brokerCfg = "config/server.properties";
        URL url = getDefaultClassLoader().getResource(brokerCfg);
        if (url == null) {
            URL rootPath = getDefaultClassLoader().getResource("");
            String runtimePath = "";
            if (rootPath != null) {
                // FileLoader
                runtimePath = rootPath.getPath() + brokerCfg;
            } else {
                // JarLoader
                runtimePath = getRuntimePath() + "!/" + brokerCfg;
            }
            // Consider as client side if 'config/server.properties' not found in current thread classloader (default: use security request header).
            log.warn(String.format("Classpath resource '%s' does not exist, it can be ignored as client side which consider using security request header.", runtimePath));
            return true;
        }
        log.info("Broker config path is: " + url.getPath());
        Properties props = new Properties();
        try {
            InputStream in = url.openStream();
            props.load(in);
            String securityKey = props.getProperty(VerifyClientVersionEnableProp);
            log.info("Broker config 'verify.client.version.enable' is: " + securityKey);
            return Boolean.parseBoolean(securityKey);
        } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
        }
    }

    public static ClassLoader getDefaultClassLoader() {
        ClassLoader cl = null;
        try {
            cl = Thread.currentThread().getContextClassLoader();
        } catch (Exception ex) {
            // Cannot access thread context ClassLoader - falling back to system class loader.
        }
        if (cl == null) {
            // No thread context class loader -> use class loader of this class.
            cl = RequestHeader.class.getClassLoader();
        }
        return cl;
    }

    /**
     * Get runtime classpath which inspired by: https://www.jianshu.com/p/b8e331840961.
     * @return current classpath
     */
    private static String getRuntimePath() {
        String classPath = RequestHeader.class.getName().replaceAll("\\.", "/") + ".class";
        URL resource = RequestHeader.class.getClassLoader().getResource(classPath);
        if (resource == null) {
            return null;
        }
        String urlString = resource.toString();
        int insidePathIndex = urlString.indexOf('!');
        boolean isInJar = insidePathIndex > -1;
        if (isInJar) {
            urlString = urlString.substring(urlString.indexOf("file:"), insidePathIndex);
            return urlString;
        }
        return urlString.substring(urlString.indexOf("file:"), urlString.length() - classPath.length());
    }

}
