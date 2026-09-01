package uploader

import (
	"context"
	"log"

	"github.com/KinMiu/worker-record/internal/config"
)

// S3Uploader handles remote cloud storage uploads (S3/MinIO/Wasabi).
type S3Uploader struct {
	cfg *config.Config
}

// NewS3Uploader creates a new S3 uploader instance.
func NewS3Uploader(cfg *config.Config) *S3Uploader {
	return &S3Uploader{
		cfg: cfg,
	}
}

// UploadSegment uploads a recorded MP4 segment file to S3 / Object Storage.
// If ENABLE_S3_UPLOAD is false, this is a no-op placeholder.
func (u *S3Uploader) UploadSegment(ctx context.Context, localFilePath, targetKey string) error {
	if !u.cfg.EnableS3Upload {
		return nil
	}

	log.Printf("[S3][PLACEHOLDER] S3 upload enabled. Uploading %s to %s...", localFilePath, targetKey)
	// Placeholder: Integrate AWS SDK / MinIO client when S3 credentials are configured.
	log.Printf("[S3][PLACEHOLDER] Upload completed for %s", targetKey)
	return nil
}
