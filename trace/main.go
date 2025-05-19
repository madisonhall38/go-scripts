package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime/pprof"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	raw "google.golang.org/api/storage/v1"
	htransport "google.golang.org/api/transport/http"

	texporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"github.com/google/uuid"
	"go.opentelemetry.io/contrib/detectors/gcp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"

	_ "google.golang.org/grpc/balancer/rls"
	_ "google.golang.org/grpc/xds/googledirectpath"
)

var (
	bucketFlag = flag.String("bucket", "mhall-golang-test", "bucket")
	api        = flag.String("api", "http2", "api; http1, http2, grpc-dp")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
	addSpans   = flag.Bool("add-spans", false, "wrap ops with app level spans")
	client     *storage.Client
)

const (
	http1 = "http1"
	http2 = "http2"
	dp    = "grpc-dp"
)

func main() {
	ctx := context.Background()
	flag.Parse()
	client = getClient(ctx)
	if client == nil {
		log.Fatalln("client is nil")
	}

	close := enableTracing(ctx)
	defer close()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}

		defer pprof.StopCPUProfile()
	}

	timetakenU, o, err := upload(ctx, *addSpans)
	if err != nil {
		log.Fatalf("upload failed: %v\n", err)
	}

	_ = o

	timetakenC := time.Duration(0)
	timetakenD := time.Duration(0)

	timetakenD, err = download(ctx, o, *addSpans)
	if err != nil {
		log.Fatalf("download failed: %v\n", err)
	}

	// timetakenC, err := listObjs(ctx, *addSpans)
	// if err != nil {
	//      log.Fatalf("download failed: %v\n", err)
	// }

	fmt.Printf("time of all ops: %v\n", timetakenC+timetakenD+timetakenU)
}

// enableTracing turns on Open Telemetry tracing with export to Cloud Trace.
func enableTracing(ctx context.Context) func() {
	exporter, err := texporter.New()
	if err != nil {
		log.Fatalf("texporter.New: %v", err)
	}

	// Identify your application using resource detection
	res, err := resource.New(ctx,
		// Use the GCP resource detector to detect information about the GCP platform
		resource.WithDetectors(gcp.NewDetector()),
		// Keep the default detectors
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceNameKey.String("my-resource-with-attr"),
		),
	)
	if errors.Is(err, resource.ErrPartialResource) || errors.Is(err, resource.ErrSchemaURLConflict) {
		log.Println(err)
	} else if err != nil {
		log.Fatalf("resource.New: %v", err)
	}

	// Create trace provider with the exporter.
	// By default it uses AlwaysSample() which samples all traces.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return func() {
		tp.ForceFlush(ctx)
		if err := tp.Shutdown(context.Background()); err != nil {
			log.Fatal(err)
		}
	}
}

func upload(ctx context.Context, withSpan bool) (runTime time.Duration, o *storage.ObjectHandle, err error) {
	var (
		bucket     = *bucketFlag
		objectName = fmt.Sprintf("%s_%s", "trace", uuid.New().String())
	)
	o = client.Bucket(bucket).Object(objectName)

	// Start span.
	if withSpan {
		ctxs, span := otel.GetTracerProvider().Tracer("go-ups").Start(ctx, "uploada")
		ctx = ctxs
		span.SetAttributes(
			attribute.KeyValue{Key: "object", Value: attribute.StringValue(objectName)},
			attribute.KeyValue{Key: "mykey", Value: attribute.StringValue(*api)},
		)
		defer span.End()
	}

	// Start timer.
	start := time.Now()
	defer func() {
		runTime = time.Since(start)
	}()

	w := o.NewWriter(ctx)

	time.Sleep(time.Second * 1)

	if _, cErr := io.CopyN(w, rand.Reader, 10*1024*1024); cErr != nil {
		w.Close()
		err = fmt.Errorf("io.CopyN: %w", cErr)
		return
	}

	if cErr := w.Close(); cErr != nil {
		err = fmt.Errorf("w.Close: %w", cErr)
		return
	}

	return
}

func download(ctx context.Context, o *storage.ObjectHandle, withSpan bool) (runTime time.Duration, err error) {
	// Start span.
	if withSpan {
		ctxs, span := otel.GetTracerProvider().Tracer("go-ups").Start(ctx, "downloads")
		ctx = ctxs
		span.SetAttributes(
			attribute.KeyValue{Key: "object", Value: attribute.StringValue(o.ObjectName())},
			attribute.KeyValue{Key: "mykey", Value: attribute.StringValue(*api)},
		)
		defer span.End()
	}

	// Start timer.
	start := time.Now()
	defer func() {
		runTime = time.Since(start)
	}()

	// 1 - user code (GCSFuse) starts a trace on ctx
	ctxa, span := otel.GetTracerProvider().Tracer("go-downs").Start(ctx, "user-span-1")
	ctx = ctxa
	span.SetAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(o.ObjectName())},
		attribute.KeyValue{Key: "mykey", Value: attribute.StringValue(*api)},
	)

	// 2 - r := NewRangeReader(ctx, {some range larger than what the kernel call was}
	r, cErr := o.NewRangeReader(ctx, 0, 1024*1024)
	if cErr != nil {
		err = fmt.Errorf("new reader: %w", cErr)
		return
	}

	// time.Sleep(time.Second * 1) // Try a small sleep here

	//3 - io.CopyN(r, {bytes 0 - 1024}) // or something similar that copies the first N bytes from the reader
	if _, cErr := io.CopyN(io.Discard, r, 1024); cErr != nil {
		r.Close()
		err = fmt.Errorf("io.Copy: %w", cErr)
		return
	}

	//4 - user code ends the trace on ctx
	span.End()

	time.Sleep(time.Second * 100)

	//5. - user code starts a new trace on ctx
	_, spanB := otel.GetTracerProvider().Tracer("go-ups").Start(ctx, "user-span-2")
	span.SetAttributes(
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(o.ObjectName())},
		attribute.KeyValue{Key: "mykey", Value: attribute.StringValue(*api)},
	)

	//5 - io.CopyN(r, ..) // next N bytes copied from r
	if _, cErr := io.CopyN(io.Discard, r, 1024*1024-1024); cErr != nil {
		r.Close()
		err = fmt.Errorf("io.Copy: %w", cErr)
		return
	}

	spanB.End()

	//copy only part of the object

	// if _, cErr := io.Copy(io.Discard, r); cErr != nil {
	//      r.Close()
	//      err = fmt.Errorf("io.Copy: %w", cErr)
	//      return
	// }

	// new trace

	// copy part of the object

	time.Sleep(time.Second * 5) // Try longer sleep

	if cErr := r.Close(); cErr != nil {
		err = fmt.Errorf("e.Close: %w", cErr)
		return
	}
	return
}

func listObjs(ctx context.Context, withSpan bool) (runTime time.Duration, err error) {
	var (
		bucket = *bucketFlag
	)

	// Start span.
	if withSpan {
		ctxs, span := otel.GetTracerProvider().Tracer("go-another").Start(ctx, "listobjsa")
		ctx = ctxs
		span.SetAttributes(
			attribute.KeyValue{Key: "mykey", Value: attribute.StringValue(*api)},
		)
		defer span.End()
	}

	// Start timer.
	start := time.Now()
	defer func() {
		runTime = time.Since(start)
	}()

	it := client.Bucket(bucket).Objects(ctx, nil)
	for {
		_, cErr := it.Next()
		if cErr == iterator.Done {
			break
		}
		if cErr != nil {
			err = fmt.Errorf("Bucket(%q).Objects: %w", bucket, cErr)
			return
		}
	}
	return
}

func getClient(ctx context.Context) *storage.Client {
	switch *api {
	case dp:
		if err := os.Setenv("GOOGLE_CLOUD_ENABLE_DIRECT_PATH_XDS", "true"); err != nil {
			log.Fatalf("set DP env var: %v", err)
		}
		client, err := storage.NewGRPCClient(ctx)
		if err != nil {
			log.Fatalf("NewGRPCClient: %v", err)
		}
		return client
	case http2:
		client, err := storage.NewClient(ctx)
		if err != nil {
			log.Fatalf("NewClient: %v", err)
		}
		return client
	case http1:
		// Use a base transport which disables HTTP/2.
		base := &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 100,
			// This disables HTTP/2 in transport.
			TLSNextProto: make(
				map[string]func(string, *tls.Conn) http.RoundTripper,
			),
		}

		trans, err := htransport.NewTransport(ctx, base, option.WithScopes(raw.DevstorageFullControlScope))
		if err != nil {
			log.Fatalf("creating transport: %v", base)
		}
		c := http.Client{Transport: trans}

		// Supply this client to storage.NewClient
		client, err = storage.NewClient(ctx, option.WithHTTPClient(&c))
		if err != nil {
			log.Fatalf("NewClient: %v", err)
		}
		return client
	default:
		log.Fatalln("invalid -api")
		return nil
	}
}
