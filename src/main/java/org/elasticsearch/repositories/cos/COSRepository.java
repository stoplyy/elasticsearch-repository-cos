package org.elasticsearch.repositories.cos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

public class COSRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(COSRepository.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    public static final String TYPE = "cos";

    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;
    private final COSService service;
    private final String bucket;

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Defaults to false.
     */
    static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);

    COSRepository(RepositoryMetaData metadata, NamedXContentRegistry namedXContentRegistry, COSService cos, ThreadPool threadpool) {
        super(metadata, COMPRESS_SETTING.get(metadata.settings()), namedXContentRegistry, threadpool);
        this.service = cos;
        String bucket = COSClientSettings.BUCKET.get(metadata.settings());
        if (bucket == null || !Strings.hasLength(bucket)) {
            throw new RepositoryException(metadata.name(), "No bucket defined for cos repository");
        }
        String basePath = COSClientSettings.BASE_PATH.get(metadata.settings());
        String app_id = COSClientSettings.APP_ID.get(metadata.settings());
        // qcloud-sdk-v5 app_id directly joined with bucket name
        if (Strings.hasLength(app_id)) {
            this.bucket = bucket + "-" + app_id;
            deprecationLogger.deprecated(
                "cos repository bucket already contain app_id, and app_id will not be supported for the cos repository in future releases"
            );
        } else {
            this.bucket = bucket;
        }

        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
            deprecationLogger.deprecated(
                "cos repository base_path trimming the leading `/`, and leading `/` will not be supported for the cos repository in future releases"
            );
        }

        if (Strings.hasLength(basePath)) {
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        this.compress = COSClientSettings.COMPRESS.get(metadata.settings());
        this.chunkSize = COSClientSettings.CHUNK_SIZE.get(metadata.settings());

        logger.trace("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket, basePath, chunkSize, compress);
    }

    @Override
    protected COSBlobStore createBlobStore() {
        return new COSBlobStore(this.service.createClient(metadata), this.bucket);
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
