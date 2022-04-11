package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.repositories.RepositoryException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

import static java.util.Collections.emptyMap;

//TODO: 考虑是否需要继承closeable，处理连接池等问题
public class COSService implements Closeable {
    private static final Logger logger = LogManager.getLogger(COSService.class);
    
    /**
     * Client settings calculated from static configuration and settings in the keystore.
     */
    private volatile Map<String, COSClientSettings> staticClientSettings = Map.of(
            "default",
            COSClientSettings.getClientSettings(Settings.EMPTY, "default")
    );
    
    /**
     * Client settings derived from those in {@link #staticClientSettings} by combining them with settings
     * in the {@link RepositoryMetadata}.
     */
    private volatile Map<Settings, COSClientSettings> derivedClientSettings = emptyMap();
   
    /**
     * Either fetches {@link COSClientSettings} for a given {@link RepositoryMetadata} from cached settings or creates them
     * by overriding static client settings from {@link #staticClientSettings} with settings found in the repository metadata.
     * @param repositoryMetadata Repository Metadata
     * @return COSClientSettings
     */
    COSClientSettings settings(RepositoryMetadata repositoryMetadata) {
        final Settings settings = repositoryMetadata.settings();
        {
            final COSClientSettings existing = derivedClientSettings.get(settings);
            if (existing != null) {
                return existing;
            }
        }
        final String clientName = COSRepository.CLIENT_NAME.get(settings);
        final COSClientSettings staticSettings = staticClientSettings.get(clientName);
        if (staticSettings != null) {
            synchronized (this) {
                final COSClientSettings existing = derivedClientSettings.get(settings);
                if (existing != null) {
                    return existing;
                }
                final COSClientSettings newSettings = staticSettings.refine(settings);
                derivedClientSettings = Maps.copyMapWithAddedOrReplacedEntry(derivedClientSettings, settings, newSettings);
                return newSettings;
            }
        }
        throw new IllegalArgumentException(
                "Unknown cos client name ["
                        + clientName
                        + "]. Existing client configs: "
                        + Strings.collectionToDelimitedString(staticClientSettings.keySet(), ",")
        );
    }
    
    
    private COSClient client;
    
    COSService(RepositoryMetadata metaData) {
        this.client = createClient(metaData);
    }
    
    private synchronized COSClient createClient(RepositoryMetadata metaData) {
        final COSClientSettings clientSettings = settings(metaData);
        
        String access_key_id = clientSettings.accessKeyId;
        String access_key_secret = clientSettings.accessKeySecret;
        String region = clientSettings.region;
        if (region == null || !Strings.hasLength(region)) {
            throw new RepositoryException(metaData.name(), "No region defined for cos repository");
        }
        String endPoint = clientSettings.endpoint;

        COSCredentials cred = new BasicCOSCredentials(access_key_id, access_key_secret);
        
        ClientConfig clientConfig = SocketAccess.doPrivileged(() -> new ClientConfig(new Region(region)));
        if (Strings.hasLength(endPoint)) {
            clientConfig.setEndPointSuffix(endPoint);
        }
        COSClient client = new COSClient(cred, clientConfig);

        return client;
    }

    public COSClient getClient() {
        return this.client;
    }

    @Override
    public void close() throws IOException {
        this.client.shutdown();
    }

}
