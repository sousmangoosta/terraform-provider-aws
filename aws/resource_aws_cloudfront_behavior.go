package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudfront"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
	"log"
	"time"
)

func resourceAwsCloudFrontBehavior() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsCloudFrontBehaviorCreate,
		Read:   resourceAwsCloudFrontBehaviorRead,
		Update: resourceAwsCloudFrontBehaviorUpdate,
		Delete: resourceAwsCloudFrontBehaviorDelete,

		Schema: map[string]*schema.Schema{
			"distribution_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"ordered_cache_behavior": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"allowed_methods": {
							Type:     schema.TypeSet,
							Required: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"cached_methods": {
							Type:     schema.TypeSet,
							Required: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"compress": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"default_ttl": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  86400,
						},
						"field_level_encryption_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"forwarded_values": {
							Type:     schema.TypeSet,
							Required: true,
							Set:      forwardedValuesHash,
							MaxItems: 1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"cookies": {
										Type:     schema.TypeSet,
										Required: true,
										Set:      cookiePreferenceHash,
										MaxItems: 1,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"forward": {
													Type:     schema.TypeString,
													Required: true,
												},
												"whitelisted_names": {
													Type:     schema.TypeList,
													Optional: true,
													Elem:     &schema.Schema{Type: schema.TypeString},
												},
											},
										},
									},
									"headers": {
										Type:     schema.TypeList,
										Optional: true,
										Elem:     &schema.Schema{Type: schema.TypeString},
									},
									"query_string": {
										Type:     schema.TypeBool,
										Required: true,
									},
									"query_string_cache_keys": {
										Type:     schema.TypeList,
										Optional: true,
										Elem:     &schema.Schema{Type: schema.TypeString},
									},
								},
							},
						},
						"lambda_function_association": {
							Type:     schema.TypeSet,
							Optional: true,
							MaxItems: 4,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"event_type": {
										Type:     schema.TypeString,
										Required: true,
									},
									"lambda_arn": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
							Set: lambdaFunctionAssociationHash,
						},
						"max_ttl": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  31536000,
						},
						"min_ttl": {
							Type:     schema.TypeInt,
							Optional: true,
							Default:  0,
						},
						"path_pattern": {
							Type:     schema.TypeString,
							Required: true,
							ForceNew: true,
						},
						"smooth_streaming": {
							Type:     schema.TypeBool,
							Optional: true,
						},
						"target_origin_id": {
							Type:     schema.TypeString,
							Required: true,
						},
						"trusted_signers": {
							Type:     schema.TypeList,
							Optional: true,
							Elem:     &schema.Schema{Type: schema.TypeString},
						},
						"viewer_protocol_policy": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}

func resourceAwsCloudFrontBehaviorCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cloudfrontconn
	d.SetId(d.Get("distribution_id").(string))
	params := &cloudfront.GetDistributionConfigInput{
		Id: aws.String(d.Id()),
	}

	resp, err := conn.GetDistributionConfig(params)
	if err != nil {
		if errcode, ok := err.(awserr.Error); ok && errcode.Code() == "NoSuchDistribution" {
			log.Printf("[WARN] No Distribution found: %s", d.Id())
			d.SetId("")
			return nil
		}

		return err
	}

	behaviors := expandCacheBehaviors(d.Get("ordered_cache_behavior").([]interface{}))

	addBehaviors(behaviors.Items, resp.DistributionConfig.CacheBehaviors)

	updateParams := &cloudfront.UpdateDistributionInput{
		Id:                 aws.String(d.Id()),
		DistributionConfig: resp.DistributionConfig,
		IfMatch:            aws.String(*resp.ETag),
	}

	_, err = conn.UpdateDistribution(updateParams)
	if err != nil {
		d.SetId("")
		return fmt.Errorf("CloudFront Distribution %s cannot be updated: %s", d.Id(), err)
	}

	return resourceAwsCloudFrontBehaviorRead(d, meta)
}

func addBehaviors(behaviors []*cloudfront.CacheBehavior, resp *cloudfront.CacheBehaviors) {
	var qty int64
	for _, v := range behaviors {
		resp.SetItems(append(resp.Items, v))
		qty++
	}
	resp.SetQuantity(*resp.Quantity + qty)
}

func resourceAwsCloudFrontBehaviorRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cloudfrontconn
	params := &cloudfront.GetDistributionConfigInput{
		Id: aws.String(d.Id()),
	}

	resp, err := conn.GetDistributionConfig(params)
	if err != nil {
		if errcode, ok := err.(awserr.Error); ok && errcode.Code() == "NoSuchDistribution" {
			log.Printf("[WARN] No Distribution found: %s", d.Id())
			d.SetId("")
			return nil
		}

		return err
	}

	behaviors := expandCacheBehaviors(d.Get("ordered_cache_behavior").([]interface{}))
	behavior := compareBehaviors(behaviors, resp.DistributionConfig)
	d.Set("ordered_cache_behavior", behavior)

	return nil
}

func compareBehaviors(behaviors *cloudfront.CacheBehaviors, resp *cloudfront.DistributionConfig) []interface{} {
	var qty int64
	s := []interface{}{}
	for _, v := range behaviors.Items {
		for _, nv := range resp.CacheBehaviors.Items {
			if *nv.PathPattern == *v.PathPattern {
				s = append(s, flattenCacheBehavior(nv))
				qty++
			}
		}
	}
	return s
}

func resourceAwsCloudFrontBehaviorUpdate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cloudfrontconn
	d.SetId(d.Get("distribution_id").(string))
	params := &cloudfront.GetDistributionConfigInput{
		Id: aws.String(d.Id()),
	}

	resp, err := conn.GetDistributionConfig(params)
	if err != nil {
		if errcode, ok := err.(awserr.Error); ok && errcode.Code() == "NoSuchDistribution" {
			log.Printf("[WARN] No Distribution found: %s", d.Id())
			return nil
		}
		return err
	}

	behaviors := expandCacheBehaviors(d.Get("ordered_cache_behavior").([]interface{}))
	updateBehaviors(behaviors.Items, resp.DistributionConfig.CacheBehaviors)

	updateParams := &cloudfront.UpdateDistributionInput{
		Id:                 aws.String(d.Id()),
		DistributionConfig: resp.DistributionConfig,
		IfMatch:            aws.String(*resp.ETag),
	}

	err = resource.Retry(1*time.Minute, func() *resource.RetryError {
		_, err := conn.UpdateDistribution(updateParams)
		if err != nil {
			// ACM and IAM certificate eventual consistency
			// InvalidViewerCertificate: The specified SSL certificate doesn't exist, isn't in us-east-1 region, isn't valid, or doesn't include a valid certificate chain.
			if isAWSErr(err, cloudfront.ErrCodeInvalidViewerCertificate, "") {
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("CloudFront Distribution %s cannot be updated: %s", d.Id(), err)
	}

	return resourceAwsCloudFrontBehaviorRead(d, meta)
}

func updateBehaviors(behaviors []*cloudfront.CacheBehavior, resp *cloudfront.CacheBehaviors) {
	var flat []*cloudfront.CacheBehavior
	for _, v := range behaviors {
		for _, nv := range resp.Items {
			if *nv.PathPattern == *v.PathPattern {
				flat = append(flat, v)
			} else {
				flat = append(flat, nv)
			}
		}
	}

	resp.SetItems(flat)
}

func resourceAwsCloudFrontBehaviorDelete(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).cloudfrontconn
	d.SetId(d.Get("distribution_id").(string))
	params := &cloudfront.GetDistributionConfigInput{
		Id: aws.String(d.Id()),
	}

	resp, err := conn.GetDistributionConfig(params)
	if err != nil {
		if errcode, ok := err.(awserr.Error); ok && errcode.Code() == "NoSuchDistribution" {
			log.Printf("[WARN] No Distribution found: %s", d.Id())
			return nil
		}
		return err
	}

	behaviors := expandCacheBehaviors(d.Get("ordered_cache_behavior").([]interface{}))

	removeBehaviors(behaviors.Items, resp.DistributionConfig.CacheBehaviors)

	updateParams := &cloudfront.UpdateDistributionInput{
		Id:                 aws.String(d.Id()),
		DistributionConfig: resp.DistributionConfig,
		IfMatch:            aws.String(*resp.ETag),
	}

	err = resource.Retry(1*time.Minute, func() *resource.RetryError {
		_, err := conn.UpdateDistribution(updateParams)
		if err != nil {
			// ACM and IAM certificate eventual consistency
			// InvalidViewerCertificate: The specified SSL certificate doesn't exist, isn't in us-east-1 region, isn't valid, or doesn't include a valid certificate chain.
			if isAWSErr(err, cloudfront.ErrCodeInvalidViewerCertificate, "") {
				return resource.RetryableError(err)
			}
			return resource.NonRetryableError(err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("CloudFront Distribution %s cannot be updated: %s", d.Id(), err)
	}

	return resourceAwsCloudFrontBehaviorRead(d, meta)
}

func removeBehaviors(behaviors []*cloudfront.CacheBehavior, resp *cloudfront.CacheBehaviors) {
	var qty int64
	var flat []*cloudfront.CacheBehavior
	for _, v := range behaviors {
		for _, nv := range resp.Items {
			if *nv.PathPattern != *v.PathPattern {
				flat = append(flat, nv)
				qty++
			}
		}
	}
	resp.SetItems(flat)
	resp.SetQuantity(qty)
}
