package org.elasticsearch.repositories.cos;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.*;
import java.util.function.Function;

import static org.elasticsearch.common.settings.Setting.*;

public class COSClientSettings {
    
    static {
        // Make sure repository plugin class is loaded before this class is used to trigger static initializer for that class which applies
        // necessary Jackson workaround
        try {
            Class.forName("org.elasticsearch.repositories.cos.COSRepositoryPlugin");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
    }
    
    // prefix for s3 client settings
    private static final String PREFIX = "cos.client.";
    
    /** Placeholder client name for normalizing client settings in the repository settings. */
    private static final String PLACEHOLDER_CLIENT = "placeholder";
    
    /** The access key (ie login id) for connecting to s3. */
    static final Setting.AffixSetting<String> ACCESS_KEY_SETTING = Setting.affixKeySetting(
            PREFIX,
            "access_key_id",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope)
    );
    
    /** The secret key (ie password) for connecting to s3. */
    static final Setting.AffixSetting<String> SECRET_KEY_SETTING = Setting.affixKeySetting(
            PREFIX,
            "access_key_secret",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope)
    );
    
    /** An override for the s3 endpoint to connect to. */
    static final Setting.AffixSetting<String> ENDPOINT_SETTING = Setting.affixKeySetting(
            PREFIX,
            "end_point",
            key -> new Setting<>(key, "", s -> s.toLowerCase(Locale.ROOT), Property.NodeScope)
    );
    
    /** An override for the s3 region to use for signing requests. */
    static final Setting.AffixSetting<String> REGION = Setting.affixKeySetting(
            PREFIX,
            "region",
            key -> new Setting<>(key, "", Function.identity(), Property.NodeScope)
    );
    
    /** The s3 endpoint the client should talk to, or empty string to use the default. */
    final String endpoint;
    
    /** Region to use for signing requests or empty string to use default. */
    final String region;
    
    final String accessKeyId;
    final String accessKeySecret;
    
    private COSClientSettings(
            String accessKeyId,
            String accessKeySecret,
            String endpoint,
            String region
    ) {
        this.accessKeyId = accessKeyId;
        this.accessKeySecret = accessKeySecret;
        this.endpoint = endpoint;
        this.region = region;
    }
    
    /**
     * Overrides the settings in this instance with settings found in repository metadata.
     *
     * @param repositorySettings found in repository metadata
     * @return COSClientSettings
     */
    COSClientSettings refine(Settings repositorySettings) {
        // Normalize settings to placeholder client settings prefix so that we can use the affix settings directly
        final Settings normalizedSettings = Settings.builder()
                .put(repositorySettings)
                .normalizePrefix(PREFIX + PLACEHOLDER_CLIENT + '.')
                .build();
        final String newAccessKeyId = getRepoSettingOrDefault(ACCESS_KEY_SETTING, normalizedSettings, accessKeyId);
        
        final String newAccessKeySecret = getRepoSettingOrDefault(SECRET_KEY_SETTING, normalizedSettings, accessKeySecret);
        
        final String newEndpoint = getRepoSettingOrDefault(ENDPOINT_SETTING, normalizedSettings, endpoint);
        
        final String newRegion = getRepoSettingOrDefault(REGION, normalizedSettings, region);
        if (Objects.equals(endpoint, newEndpoint)
                && Objects.equals(region, newRegion)
                && Objects.equals(accessKeyId, newAccessKeyId)
                && Objects.equals(accessKeySecret, newAccessKeySecret)) {
            return this;
        }
        return new COSClientSettings(
                newAccessKeyId,
                newAccessKeySecret,
                newEndpoint,
                newRegion
        );
    }
    
    /**
     * Load all client settings from the given settings.
     *
     * Note this will always at least return a client named "default".
     */
    static Map<String, COSClientSettings> load(Settings settings) {
        final Set<String> clientNames = settings.getGroups(PREFIX).keySet();
        final Map<String, COSClientSettings> clients = new HashMap<>();
        for (final String clientName : clientNames) {
            clients.put(clientName, getClientSettings(settings, clientName));
        }
        if (clients.containsKey("default") == false) {
            // this won't find any settings under the default client,
            // but it will pull all the fallback static settings
            clients.put("default", getClientSettings(settings, "default"));
        }
        return Collections.unmodifiableMap(clients);
    }
    
    // pkg private for tests
    /** Parse settings for a single client. */
    static COSClientSettings getClientSettings(final Settings settings, final String clientName) {
        return new COSClientSettings(
                getConfigValue(settings, clientName, ACCESS_KEY_SETTING),
                getConfigValue(settings, clientName, SECRET_KEY_SETTING),
                getConfigValue(settings, clientName, ENDPOINT_SETTING),
                getConfigValue(settings, clientName, REGION)
        );
    }
    
    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final COSClientSettings that = (COSClientSettings) o;
        return Objects.equals(endpoint, that.endpoint)
                && Objects.equals(region, that.region)
                && Objects.equals(accessKeyId, that.accessKeyId)
                && Objects.equals(accessKeySecret, that.accessKeySecret);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(
                endpoint,
                region,
                accessKeyId,
                accessKeySecret
        );
    }
    
    private static <T> T getConfigValue(Settings settings, String clientName, Setting.AffixSetting<T> clientSetting) {
        final Setting<T> concreteSetting = clientSetting.getConcreteSettingForNamespace(clientName);
        return concreteSetting.get(settings);
    }
    
    private static <T> T getRepoSettingOrDefault(Setting.AffixSetting<T> setting, Settings normalizedSettings, T defaultValue) {
        if (setting.getConcreteSettingForNamespace(PLACEHOLDER_CLIENT).exists(normalizedSettings)) {
            return getConfigValue(normalizedSettings, PLACEHOLDER_CLIENT, setting);
        }
        return defaultValue;
    }
    
}
