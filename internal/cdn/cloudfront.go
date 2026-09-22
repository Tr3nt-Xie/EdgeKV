package cdn

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
)

// CloudFront invalidates paths on a CloudFront distribution. Credentials come
// from the default AWS chain; on EC2 that is the instance role, which needs
// only cloudfront:CreateInvalidation on this one distribution.
//
// A CloudFront invalidation typically takes from a few seconds up to a minute
// or so to reach every edge location, and the first 1000 paths per month are
// free. Both facts are why the TTL, not this call, defines the staleness bound.
type CloudFront struct {
	DistributionID string
	client         *cloudfront.Client
}

// NewCloudFront loads AWS configuration from the environment.
func NewCloudFront(ctx context.Context, distributionID string) (*CloudFront, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("cdn: load AWS config: %w", err)
	}
	return &CloudFront{DistributionID: distributionID, client: cloudfront.NewFromConfig(cfg)}, nil
}

func (c *CloudFront) Name() string { return "cloudfront" }

func (c *CloudFront) Invalidate(ctx context.Context, paths []string) error {
	_, err := c.client.CreateInvalidation(ctx, &cloudfront.CreateInvalidationInput{
		DistributionId: aws.String(c.DistributionID),
		InvalidationBatch: &types.InvalidationBatch{
			// CallerReference makes the request idempotent on AWS's side.
			CallerReference: aws.String(fmt.Sprintf("edgekv-%d", time.Now().UnixNano())),
			Paths: &types.Paths{
				Quantity: aws.Int32(int32(len(paths))),
				Items:    paths,
			},
		},
	})
	return err
}
