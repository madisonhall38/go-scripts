package main

import (
	"context"
	"fmt"
	"log"

	control "cloud.google.com/go/storage/control/apiv2"
	"cloud.google.com/go/storage/control/apiv2/controlpb"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

func main() {
	ctx := context.Background()

	scope := "https://www.googleapis.com/auth/devstorage.full_control"
	tokenSrc, err := google.DefaultTokenSource(ctx, scope)
	if err != nil {
		log.Fatalf("JWTAccessTokenSourceWithScope: %w", err)
	}

	// Create client options
	clientOpts := []option.ClientOption{option.WithTokenSource(tokenSrc)}

	controlClient, err := control.NewStorageControlClient(ctx, clientOpts...)
	if err != nil {
		log.Fatalf("failed to create control client: %v", err)
	}

	fmt.Println("Successfully created control client:", controlClient)

	// Make tbe GetStorageLayout API call
	req := &controlpb.GetStorageLayoutRequest{
		// Define your request parameters here.  For example:
		// Name: "projects/storage-sdks-madisonhall/buckets/mhall-golang-test/storageLayout",
		Name: "projects/_/buckets/mhall-golang-test/storageLayout",
	}

	layout, err := controlClient.GetStorageLayout(ctx, req)
	if err != nil {
		log.Fatalf("failed to get storage layout: %v", err)
	}

	fmt.Printf("Storage Layout: %v\n", layout)
}
