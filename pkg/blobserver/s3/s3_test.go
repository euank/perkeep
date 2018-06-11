/*
Copyright 2014 The Perkeep Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package s3

import (
	"context"
	"flag"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/storagetest"
	"perkeep.org/pkg/schema"

	"go4.org/jsonconfig"
)

var (
	key          = flag.String("s3_key", "", "AWS access Key ID")
	secret       = flag.String("s3_secret", "", "AWS access secret")
	bucket       = flag.String("s3_bucket", "", "Bucket name to use for testing. If empty, testing is skipped. If non-empty, it must have zero items in it.")
	flagTestData = flag.String("testdata", "", "Optional directory containing some files to write to the bucket, for additional tests.")
)

var ctxbg = context.Background()

func TestS3(t *testing.T) {
	testStorage(t, "")
}

func TestS3WithBucketDir(t *testing.T) {
	testStorage(t, "/bl/obs/")
}

func TestS3WriteFiles(t *testing.T) {
	if *flagTestData == "" {
		t.Skipf("testdata dir not specified, skipping test.")
	}
	sto, err := newFromConfig(nil, jsonconfig.Obj{
		"aws_access_key":        *key,
		"aws_secret_access_key": *secret,
		"bucket":                *bucket,
	})
	if err != nil {
		t.Fatalf("newFromConfig error: %v", err)
	}
	dir, err := os.Open(*flagTestData)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	names, err := dir.Readdirnames(-1)
	for _, name := range names {
		f, err := os.Open(filepath.Join(*flagTestData, name))
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close() // assuming there aren't that many files.
		if _, err := schema.WriteFileFromReaderWithModTime(ctxbg, sto, name, time.Now(), f); err != nil {
			t.Fatalf("Error while writing %v to S3: %v", name, err)
		}
		t.Logf("Wrote %v successfully to S3", name)
	}
}

func testStorage(t *testing.T, bucketDir string) {
	if *bucket == "" || *key == "" || *secret == "" {
		t.Skip("Skipping test because at least one of -s3_key, -s3_secret, or -s3_bucket flags has not been provided.")
	}

	bucketWithDir := path.Join(*bucket, bucketDir)
	storagetest.Test(t, func(t *testing.T) (sto blobserver.Storage, cleanup func()) {
		sto, err := newFromConfig(nil, jsonconfig.Obj{
			"aws_access_key":        *key,
			"aws_secret_access_key": *secret,
			"bucket":                bucketWithDir,
			"cacheSize":             float64(0),
		})
		if err != nil {
			t.Fatalf("newFromConfig error: %v", err)
		}
		if !testing.Short() {
			log.Printf("Warning: this test does many serial operations. Without the go test -short flag, this test will be very slow.")
		}

		if bucketWithDir != *bucket {
			// Adding "a", and "c" objects in the bucket to make sure objects out of the
			// "directory" are not touched and have no influence.
			for _, key := range []string{"a", "c"} {
				if err != nil {
					t.Fatalf("could not insert object %s in bucket %v: %v", key, sto.(*s3Storage).bucket, err)
				}
				if _, err := sto.(*s3Storage).client.PutObject(&s3.PutObjectInput{
					Bucket: &sto.(*s3Storage).bucket,
					Key:    aws.String(key),
					Body:   strings.NewReader(key),
				}); err != nil {
					t.Fatalf("could not insert object %s in bucket %v: %v", key, sto.(*s3Storage).bucket, err)
				}
			}
		}
		clearBucket := func(beforeTests bool) func() {
			return func() {
				var all []blob.Ref
				blobserver.EnumerateAll(ctxbg, sto, func(sb blob.SizedRef) error {
					t.Logf("Deleting: %v", sb.Ref)
					all = append(all, sb.Ref)
					return nil
				})
				if err := sto.RemoveBlobs(ctxbg, all); err != nil {
					t.Fatalf("Error removing blobs during cleanup: %v", err)
				}
				if beforeTests {
					return
				}
				if bucketWithDir != *bucket {
					// checking that "a" and "c" at the root were left untouched.
					for _, key := range []string{"a", "c"} {
						if _, err := sto.(*s3Storage).client.GetObject(&s3.GetObjectInput{
							Bucket: &sto.(*s3Storage).bucket,
							Key:    aws.String(key),
						}); err != nil {
							t.Fatalf("could not find object %s after tests: %v", key, err)
						}
						if _, err := sto.(*s3Storage).client.DeleteObject(&s3.DeleteObjectInput{
							Bucket: &sto.(*s3Storage).bucket,
							Key:    aws.String(key),
						}); err != nil {
							t.Fatalf("could not remove object %s after tests: %v", key, err)
						}
					}
				}
			}
		}
		clearBucket(true)()
		return sto, clearBucket(false)
	})
}
