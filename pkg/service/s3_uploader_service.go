package service

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/gofiber/fiber/v2/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/KinMiu/worker-record/config"
)

// S3UploaderService handles remote cloud object storage uploads (MinIO / S3).
type S3UploaderService struct {
	client     *minio.Client
	initOnce   sync.Once
	initErr    error
	bucketInit bool
	mu         sync.Mutex
}

// NewS3UploaderService creates an instance of S3UploaderService.
func NewS3UploaderService() *S3UploaderService {
	return &S3UploaderService{}
}

func (u *S3UploaderService) initClient() error {
	u.initOnce.Do(func() {
		if !config.ENABLE_S3_UPLOAD.GetValueBool(false) {
			return
		}

		endpoint := config.S3_ENDPOINT.GetValueOrDefault("127.0.0.1:9000")
		accessKey := config.S3_ACCESS_KEY.GetValue()
		secretKey := config.S3_SECRET_KEY.GetValue()
		useSSL := config.S3_USE_SSL.GetValueBool(false)
		region := config.S3_REGION.GetValueOrDefault("us-east-1")

		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: useSSL,
			Region: region,
		})
		if err != nil {
			u.initErr = fmt.Errorf("failed to initialize S3 client: %w", err)
			log.Errorf("[S3] %v", u.initErr)
			return
		}

		u.client = client
		log.Infof("[S3] Connected to S3/MinIO endpoint: %s (Bucket: %s, SSL: %v)",
			endpoint, config.S3_BUCKET.GetValueOrDefault("recordings"), useSSL)
	})

	return u.initErr
}

func (u *S3UploaderService) ensureBucket(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.bucketInit {
		return nil
	}

	bucket := config.S3_BUCKET.GetValueOrDefault("recordings")
	region := config.S3_REGION.GetValueOrDefault("us-east-1")

	exists, err := u.client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("error checking bucket %s: %w", bucket, err)
	}

	if !exists {
		err = u.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: region})
		if err != nil {
			return fmt.Errorf("error creating bucket %s: %w", bucket, err)
		}
		log.Infof("[S3] Successfully created bucket: %s", bucket)

		policy := fmt.Sprintf(`{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Effect": "Allow",
					"Principal": "*",
					"Action": ["s3:GetObject"],
					"Resource": ["arn:aws:s3:::%s/*"]
				}
			]
		}`, bucket)

		if err := u.client.SetBucketPolicy(ctx, bucket, policy); err != nil {
			log.Warnf("[S3] Could not set public read policy for bucket %s: %v", bucket, err)
		} else {
			log.Infof("[S3] Public download policy applied to bucket: %s", bucket)
		}
	}

	u.bucketInit = true
	return nil
}

// UploadSegment uploads a recorded MP4 segment file to S3 / MinIO Object Storage.
func (u *S3UploaderService) UploadSegment(ctx context.Context, localFilePath, targetKey string) error {
	if !config.ENABLE_S3_UPLOAD.GetValueBool(false) {
		return nil
	}

	if err := u.initClient(); err != nil {
		return err
	}

	if u.client == nil {
		return fmt.Errorf("S3 client is not initialized")
	}

	if err := u.ensureBucket(ctx); err != nil {
		return err
	}

	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return fmt.Errorf("local file not found: %w", err)
	}

	bucket := config.S3_BUCKET.GetValueOrDefault("recordings")
	opts := minio.PutObjectOptions{
		ContentType: "video/mp4",
	}

	log.Infof("[S3] Uploading %s (%.2f MB) -> s3://%s/%s...",
		fileInfo.Name(), float64(fileInfo.Size())/(1024*1024), bucket, targetKey)

	info, err := u.client.FPutObject(ctx, bucket, targetKey, localFilePath, opts)
	if err != nil {
		return fmt.Errorf("failed to upload %s to S3: %w", targetKey, err)
	}

	log.Infof("[S3] Upload completed: s3://%s/%s (Size: %.2f MB, ETag: %s)",
		bucket, targetKey, float64(info.Size)/(1024*1024), info.ETag)

	return nil
}
