package uploader

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/KinMiu/worker-record/internal/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Uploader handles remote cloud storage uploads (MinIO / S3 / Wasabi / R2).
type S3Uploader struct {
	cfg        *config.Config
	client     *minio.Client
	initOnce   sync.Once
	initErr    error
	bucketInit bool
	mu         sync.Mutex
}

// NewS3Uploader creates a new S3 uploader instance.
func NewS3Uploader(cfg *config.Config) *S3Uploader {
	return &S3Uploader{
		cfg: cfg,
	}
}

// initClient initializes the MinIO / S3 client connection.
func (u *S3Uploader) initClient() error {
	u.initOnce.Do(func() {
		if !u.cfg.EnableS3Upload {
			return
		}

		client, err := minio.New(u.cfg.S3Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(u.cfg.S3AccessKey, u.cfg.S3SecretKey, ""),
			Secure: u.cfg.S3UseSSL,
			Region: u.cfg.S3Region,
		})
		if err != nil {
			u.initErr = fmt.Errorf("failed to initialize S3 client: %w", err)
			log.Printf("[S3][ERROR] %v", u.initErr)
			return
		}

		u.client = client
		log.Printf("[S3] Connected to S3/MinIO endpoint: %s (Bucket: %s, SSL: %v)",
			u.cfg.S3Endpoint, u.cfg.S3Bucket, u.cfg.S3UseSSL)
	})

	return u.initErr
}

// ensureBucket verifies that the target bucket exists, creating it with public read policy if needed.
func (u *S3Uploader) ensureBucket(ctx context.Context) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.bucketInit {
		return nil
	}

	exists, err := u.client.BucketExists(ctx, u.cfg.S3Bucket)
	if err != nil {
		return fmt.Errorf("error checking bucket %s: %w", u.cfg.S3Bucket, err)
	}

	if !exists {
		err = u.client.MakeBucket(ctx, u.cfg.S3Bucket, minio.MakeBucketOptions{Region: u.cfg.S3Region})
		if err != nil {
			return fmt.Errorf("error creating bucket %s: %w", u.cfg.S3Bucket, err)
		}
		log.Printf("[S3] Successfully created bucket: %s", u.cfg.S3Bucket)

		// Set bucket policy to allow anonymous read / video playback
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
		}`, u.cfg.S3Bucket)

		if err := u.client.SetBucketPolicy(ctx, u.cfg.S3Bucket, policy); err != nil {
			log.Printf("[S3][WARNING] Could not set public read policy for bucket %s: %v", u.cfg.S3Bucket, err)
		} else {
			log.Printf("[S3] Public download policy applied to bucket: %s", u.cfg.S3Bucket)
		}
	}

	u.bucketInit = true
	return nil
}

// UploadSegment uploads a recorded MP4 segment file to S3 / MinIO Object Storage.
func (u *S3Uploader) UploadSegment(ctx context.Context, localFilePath, targetKey string) error {
	if !u.cfg.EnableS3Upload {
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

	// Verify local file exists
	fileInfo, err := os.Stat(localFilePath)
	if err != nil {
		return fmt.Errorf("local file not found: %w", err)
	}

	opts := minio.PutObjectOptions{
		ContentType: "video/mp4",
	}

	log.Printf("[S3] Uploading %s (%.2f MB) -> s3://%s/%s...",
		fileInfo.Name(), float64(fileInfo.Size())/(1024*1024), u.cfg.S3Bucket, targetKey)

	info, err := u.client.FPutObject(ctx, u.cfg.S3Bucket, targetKey, localFilePath, opts)
	if err != nil {
		return fmt.Errorf("failed to upload %s to S3: %w", targetKey, err)
	}

	log.Printf("[S3] Upload completed: s3://%s/%s (Size: %.2f MB, ETag: %s)",
		u.cfg.S3Bucket, targetKey, float64(info.Size)/(1024*1024), info.ETag)

	// Optional local file cleanup
	if u.cfg.S3DeleteLocal {
		if err := os.Remove(localFilePath); err != nil {
			log.Printf("[S3][WARNING] Failed to delete local file %s after upload: %v", localFilePath, err)
		} else {
			log.Printf("[S3] Local file removed: %s", localFilePath)
		}
	}

	return nil
}
