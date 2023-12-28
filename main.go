package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var (
	endpoint, accessKey, secretKey                   string
	remoteEndpoint, remoteAccessKey, remoteSecretKey string
	bucket, prefix                                   string
	insecure, bypassGovernance                       bool
	objectsDeleted                                   int64
)

func main() {
	flag.StringVar(&endpoint, "endpoint", "", "S3 endpoint URL")
	flag.StringVar(&accessKey, "access-key", "", "S3 Access Key")
	flag.StringVar(&secretKey, "secret-key", "", "S3 Secret Key")
	flag.StringVar(&remoteEndpoint, "remote-endpoint", "", "S3 endpoint URL of the remote target")
	flag.StringVar(&remoteAccessKey, "remote-access-key", "", "S3 Access Key of the remote target")
	flag.StringVar(&remoteSecretKey, "remote-secret-key", "", "S3 Secret Key of the remote target")
	flag.StringVar(&bucket, "bucket", "", "Select a specific bucket")
	flag.StringVar(&prefix, "prefix", "", "Select an object/prefix")
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

	if remoteEndpoint == "" {
		log.Fatalln("remote endpoint is not provided")
	}

	if remoteAccessKey == "" {
		log.Fatalln("remote access key is not provided")
	}

	if remoteSecretKey == "" {
		log.Fatalln("remote secret key is not provided")
	}

	s3Client := getS3Client(endpoint, accessKey, secretKey, insecure)
	remoteS3Client := getS3Client(remoteEndpoint, remoteAccessKey, remoteSecretKey, insecure)

	ctx := context.Background()

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("\nDeleted %v objects...", objectsDeleted)
			}
		}
	}()

	for obj := range s3Client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Recursive:    true,
		Prefix:       prefix,
		WithVersions: true,
		WithMetadata: true,
	}) {
		if obj.Err != nil {
			log.Fatalln("unable to list with error:", obj.Err)
			return
		}
		if obj.IsDeleteMarker && obj.IsLatest {
			// the latest version of the object is a delete marker
			// Fetching the remote object to compare and decide...
			remoteObject, err := remoteS3Client.GetObject(ctx, bucket, obj.Key, minio.GetObjectOptions{})
			if err != nil {
				log.Printf("unable to get the object %s from the remote; %v\n", obj.Key, err)
				continue
			}
			if remoteObject != nil {
				oi, err := remoteObject.Stat()
				if err == nil {
					if oi.LastModified.After(obj.LastModified) {
						// the remote object is the latest, skipping this.
						continue
					}
				}
				if err != nil {
					if minio.ToErrorResponse(err).Code != "NoSuchKey" {
						log.Printf("unable to stat the remote object %s; %v\n", obj.Key, err)
						continue
					}
				}
			}
			if err := s3Client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{
				ForceDelete:      true,
				GovernanceBypass: bypassGovernance,
			}); err != nil {
				log.Printf("unable to delete the object from source: %v; %v\n", obj.Key, err)
				continue
			}
			objectsDeleted++
			// If the source object is latest and has a delete marker
			// then delete the object from remote too.
			if remoteObject != nil {
				if err := remoteS3Client.RemoveObject(ctx, bucket, obj.Key, minio.RemoveObjectOptions{
					ForceDelete:      true,
					GovernanceBypass: bypassGovernance,
				}); err != nil {
					log.Printf("unable to delete the object from target: %v; %v\n", obj.Key, err)
				}
			}
		}
	}
	fmt.Printf("\nDeleted %v objects...", objectsDeleted)
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
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = insecure
	}

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

func logProgress(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fmt.Printf("\nDeleted %v objects...", objectsDeleted)
		}
	}
}
