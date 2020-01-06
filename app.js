const ApiBuilder = require('claudia-api-builder');
const AWS = require('aws-sdk');

const api = new ApiBuilder();
module.exports = api;

const S3_BUCKET_FOR_URBAN = '${bucket_name}';

const s3 = new AWS.S3({
  apiVersion: '2006-03-01',
  signatureVersion: 'v4'
});

api.get('/lidar_data/object_versions', async (request) => {
  const bucket = S3_BUCKET_FOR_URBAN;
  const prefix = 'lidar_data';

  return await queryObjectVersions(bucket, prefix);
});

api.get('/footprint_data/object_versions', async (request) => {
  const bucket = S3_BUCKET_FOR_URBAN;
  const prefix = 'footprint_data';

  return await queryObjectVersions(bucket, prefix);
});

api.get('download_url', async (request) => {
  const bucketName = S3_BUCKET_FOR_URBAN;
  const key = decodeURIComponent(request.queryString.Key);

  return await getDownloadUrl(bucketName, key);
});

api.get('upload_url', async (request) => {
  const bucketName = S3_BUCKET_FOR_URBAN;
  const Key = decodeURIComponent(request.queryString.Key);

  return await getUploadUrl(bucketName, Key);
});

const queryObjectVersions = async (bucket, prefix) => {
  const s3Result = await listObjectVersions(bucket, prefix);
  const ret = s3Result.Versions;
  ret.shift();

  for (const val of ret) {
    val.Uri = `s3://${bucket}/${val.Key}`;
  }

  return new ApiBuilder.ApiResponse(ret, {
    'Content-Type': 'application/json'
  }, 200);
}

const getUploadUrl = async (Bucket, Key) => {
  const params = {
    Bucket,
    Key,
    Expires: 3600,
    ContentType: 'application/json'
  };

  try {
    const url = await new Promise((resolve, reject) => {
      s3.getSignedUrl('putObject', params, (err, url) => {
        if (err) {
          reject(err);
        }
        resolve({
          Key,
          Url: url
        });
      });
    });

    return url;
  } catch (err) {
    logger.error('s3 getObject, get signedUrl failed.');
    throw err;
  }
}

const getDownloadUrl = async (bucketName, objectName) => {
  const params = {
    Bucket: bucketName,
    Key: objectName,
    Expires: 3600
  };

  try {
    const url = await new Promise((resolve, reject) => {
      s3.getSignedUrl('getObject', params, (err, url) => {
        if (err) {
          reject(err);
        }
        resolve({
          Key: objectName,
          Url: url
        });
      });
    });

    return url;
  } catch (err) {
    logger.error('s3 getObject, get signedUrl failed.');
    throw err;
  }
}

const listObjectVersions = (srcBucket, prefix) => {
  return new Promise((resolve, reject) => {
    const params = {
      Bucket: srcBucket,
      Prefix: prefix
    };

    s3.listObjectVersions(params, (err, data) => {
      if (err) {
        console.log(err, err.stack);
        reject();
      } else {
        resolve(data);
      }
    });
  });
}
