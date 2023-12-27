package main

import (
	"context"
	"flag"
	"log"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	endpoint, accessKey, secretKey string
	bucket, object                 string
	insecure, bypassGovernance     bool
)

func main() {
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint URL")
	flag.StringVar(&accessKey, "access-key", "", "S3 Access Key")
	flag.StringVar(&secretKey, "secret-key", "", "S3 Secret Key")
	flag.StringVar(&bucket, "bucket", "", "Select a specific bucket")
	flag.StringVar(&object, "object", "", "Select an object")
	flag.BoolVar(&insecure, "insecure", false, "Disable TLS verification")
	flag.BoolVar(&bypassGovernance, "bypass-governance", false, "Bypass governance on deletion")
	flag.Parse()

	if endpoint == "" {
		log.Fatalln("endpoint is not provided")
	}

	if accessKey == "" {
		log.Fatalln("access key is not provided")
	}

	if secretKey == "" {
		log.Fatalln("secret key is not provided")
	}

	if bucket == "" {
		log.Fatalln("bucket should not be empty")
	}

	s3Client := getS3Client(endpoint, accessKey, secretKey, insecure)

	ctx := context.Background()
	for obj := range s3Client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive:    true,
		Prefix:       object,
		WithVersions: true,
	}) {
		if obj.Err != nil {
			log.Fatalln("FAILED: LIST with error:", obj.Err)
			return
		}
		if obj.IsDeleteMarker && obj.IsLatest {
			if err := s3Client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{
				ForceDelete:      true,
				GovernanceBypass: bypassGovernance,
			}); err != nil {
				log.Println("unable to delete the object: %v; %v", obj.Key, err)
			}
		}
	}

}

func getS3Client(endpoint string, accessKey string, secretKey string, insecure bool) *minio.Client {
	u, err := url.Parse(endpoint)
	if err != nil {
		log.Fatalln(err)
	}

	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		log.Fatalln(err)
	}
	transport.TLSClientConfig.InsecureSkipVerify = insecure

	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		log.Fatalln(err)
	}
	return s3Client
}
