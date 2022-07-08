package org.elasticsearch.repositories.cos;

import static java.util.Collections.emptyMap;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.RepositoryException;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import com.qcloud.cos.utils.StringUtils;

public class COSService {

    public static final ByteSizeValue MAX_SINGLE_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    private Map<String, COSClient> clientMap = new HashMap<>();

    private Map<String, COSClientSecretSettings> secretSettings = emptyMap();

    private final Logger logger = LogManager.getLogger(COSService.class);

    public COSService(Settings settings) {
        // eagerly load client settings so that secure settings are read
        final Map<String, COSClientSecretSettings> clientsSettings = COSClientSecretSettings.load(settings);
        refreshAndClearCache(clientsSettings);
        logger.info("cos service init finished.");
    }

    public Map<String, COSClientSecretSettings> refreshAndClearCache(Map<String, COSClientSecretSettings> clientsSettings) {
        final Map<String, COSClientSecretSettings> prevSettings = this.secretSettings;
        this.secretSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
        logger.info("cos service refresh settings finished.");
        return prevSettings;
    }

    public COSClient getClient(RepositoryMetadata metaData) {
        Tuple<String, String> secret = getSecret(metaData);
        String region = COSClientSettings.REGION.get(metaData.settings());
        if (region == null || !Strings.hasLength(region)) {
            throw new RepositoryException(metaData.name(), "No region defined for cos repository");
        }
        String endPoint = COSClientSettings.END_POINT.get(metaData.settings());
        ClientMetaData clientMetaData = new ClientMetaData(secret, region, endPoint);

        if (this.clientMap.containsKey(clientMetaData.getKey())) {
            return this.clientMap.get(clientMetaData.getKey());
        }
        return createClient(clientMetaData);
    }

    private synchronized COSClient createClient(ClientMetaData clientMetaData) {
        COSCredentials cred = new BasicCOSCredentials(clientMetaData.access_key_id, clientMetaData.access_key_secret);
        ClientConfig clientConfig = SocketAccess.doPrivileged(() -> new ClientConfig(new Region(clientMetaData.region)));
        if (Strings.hasLength(clientMetaData.endPoint)) {
            clientConfig.setEndPointSuffix(clientMetaData.endPoint);
        }
        COSClient client = new COSClient(cred, clientConfig);
        this.clientMap.put(clientMetaData.getKey(), client);
        logger.debug("create cos client success cacheKey[{}]. current clientcount:[{}]. ", clientMetaData.getKey(), this.clientMap.size());
        return client;
    }

    private Tuple<String, String> getSecret(RepositoryMetadata metaData) {
        // meta setting first
        String access_key_id = COSClientSettings.ACCESS_KEY_ID.get(metaData.settings());
        String access_key_secret = COSClientSettings.ACCESS_KEY_SECRET.get(metaData.settings());
        if (access_key_id == null
            || !Strings.hasLength(access_key_id)
            || access_key_secret == null
            || !Strings.hasLength(access_key_secret)) {
            // secret setting
            String account = COSClientSettings.ACCOUNT.get(metaData.settings());
            final COSClientSecretSettings cosSecretSetting = this.secretSettings.get(account);
            if (cosSecretSetting == null) {
                throw new SettingsException("Unable to find cos repo secret settings with name [" + account + "]");
            }
            access_key_id = cosSecretSetting.getSecretId();
            access_key_secret = cosSecretSetting.getSecretKey();
            logger.debug("account:[" + account + "] get secret successful. id:[" + access_key_id + "] secret:[" + access_key_secret + "]");
        }
        return new Tuple<>(access_key_id, access_key_secret);
    }

    class ClientMetaData {
        ClientMetaData(Tuple<String, String> secret, String region, String endPoint) {
            this.access_key_id = secret.v1();
            this.access_key_secret = secret.v2();
            this.region = region;
            this.endPoint = endPoint;
        }

        String access_key_id;
        String access_key_secret;
        String region;
        String endPoint;

        String getKey() {
            return StringUtils.join(
                ":",
                access_key_id,
                access_key_secret,
                region == null ? "null" : region,
                endPoint == null ? "null" : endPoint
            );
        }
    }

}
