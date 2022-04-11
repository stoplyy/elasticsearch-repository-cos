package org.elasticsearch.repositories.cos;

import java.util.*;

import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.xcontent.NamedXContentRegistry;

/**
 * Created by Ethan-Zhang on 30/03/2018.
 */
public class COSRepositoryPlugin extends Plugin implements RepositoryPlugin {
    
    // proxy method for testing
    protected COSRepository createRepository(
            final RepositoryMetadata metadata,
            final NamedXContentRegistry registry,
            final ClusterService clusterService,
            final BigArrays bigArrays,
            final RecoverySettings recoverySettings
    ) {
        return new COSRepository(metadata, registry,
                new COSService(metadata), clusterService, bigArrays, recoverySettings);
    }
    
    @Override
    public Map<String, Repository.Factory> getRepositories(
            final Environment env,
            final NamedXContentRegistry registry,
            final ClusterService clusterService,
            final BigArrays bigArrays,
            final RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
                COSRepository.TYPE,
                metadata -> createRepository(metadata, registry, clusterService, bigArrays, recoverySettings)
        );
    }
    
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
                // named s3 client configuration settings
                COSClientSettings.ACCESS_KEY_SETTING,
                COSClientSettings.SECRET_KEY_SETTING,
                COSClientSettings.ENDPOINT_SETTING,
                COSClientSettings.REGION
        );
    }

}