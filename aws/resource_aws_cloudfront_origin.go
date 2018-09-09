package aws

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/cloudfront"
	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"log"
	"time"
)

func resourceAwsCloudFrontOrigin() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsCloudFrontOriginCreate,
		Read:   resourceAwsCloudFrontOriginRead,
		Update: resourceAwsCloudFrontOriginUpdate,
		Delete: resourceAwsCloudFrontOriginDelete,

		Schema: map[string]*schema.Schema{
			"distribution_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"origin": {
				Type:     schema.TypeSet,
				Required: true,
				Set:      originHash,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"custom_origin_config": {
							Type:          schema.TypeSet,
							Optional:      true,
							ConflictsWith: []string{"origin.s3_origin_config"},
							Set:           customOriginConfigHash,
							MaxItems:      1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"http_port": {
										Type:     schema.TypeInt,
										Required: true,
									},
									"https_port": {
										Type:     schema.TypeInt,
										Required: true,
									},
									"origin_keepalive_timeout": {
										Type:     schema.TypeInt,
										Optional: true,
										Default:  5,
									},
									"origin_read_timeout": {
										Type:     schema.TypeInt,
										Optional: true,
										Default:  30,
									},
									"origin_protocol_policy": {
										Type:     schema.TypeString,
										Required: true,
									},
									"origin_ssl_protocols": {
										Type:     schema.TypeList,
										Required: true,
										Elem:     &schema.Schema{Type: schema.TypeString},
									},
								},
							},
						},
						"domain_name": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"custom_header": {
							Type:     schema.TypeSet,
							Optional: true,
							Set:      originCustomHeaderHash,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Required: true,
									},
									"value": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
						"origin_id": {
							Type:         schema.TypeString,
							Required:     true,
							ValidateFunc: validation.NoZeroValues,
						},
						"origin_path": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"s3_origin_config": {
							Type:          schema.TypeSet,
							Optional:      true,
							ConflictsWith: []string{"origin.custom_origin_config"},
							Set:           s3OriginConfigHash,
							MaxItems:      1,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"origin_access_identity": {
										Type:     schema.TypeString,
										Required: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceAwsCloudFrontOriginCreate(d *schema.ResourceData, meta interface{}) error {
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

	origins := expandOrigins(d.Get("origin").(*schema.Set))

	addOrigins(origins.Items, resp.DistributionConfig.Origins)

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

	return resourceAwsCloudFrontOriginRead(d, meta)
}

func addOrigins(origins []*cloudfront.Origin, resp *cloudfront.Origins) {
	var qty int64
	for _, v := range origins {
		resp.SetItems(append(resp.Items, v))
		qty++
	}
	resp.SetQuantity(*resp.Quantity + qty)
}

func resourceAwsCloudFrontOriginRead(d *schema.ResourceData, meta interface{}) error {
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

	origins := expandOrigins(d.Get("origin").(*schema.Set))

	origin := compareOrigins(origins, resp)

	d.Set("origin", origin)

	return nil
}

func compareOrigins(origins *cloudfront.Origins, resp *cloudfront.GetDistributionConfigOutput) *schema.Set {
	s := []interface{}{}
	for _, v := range origins.Items {
		for _, nv := range resp.DistributionConfig.Origins.Items {
			if *nv.Id == *v.Id {
				s = append(s, flattenOrigin(nv))
			}
		}
	}
	return schema.NewSet(originHash, s)
}

func resourceAwsCloudFrontOriginUpdate(d *schema.ResourceData, meta interface{}) error {
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

	origins := expandOrigins(d.Get("origin").(*schema.Set))
	updateOrigins(origins.Items, resp.DistributionConfig.Origins)

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

	return resourceAwsCloudFrontOriginRead(d, meta)
}

func updateOrigins(origins []*cloudfront.Origin, resp *cloudfront.Origins) {
	var qty int64
	var flat *schema.Set
	flat = flattenOrigins(resp)
	for _, v := range origins {
		for _, nv := range resp.Items {
			if *nv.Id == *v.Id {
				if flat.Contains(flattenOrigin(nv)) {
					flat.Remove(flattenOrigin(nv))
					flat.Add(flattenOrigin(v))
				}
			}
		}
	}

	expand := expandOrigins(flat)
	resp.SetItems(expand.Items)
	resp.SetQuantity(*resp.Quantity - qty)
}

func resourceAwsCloudFrontOriginDelete(d *schema.ResourceData, meta interface{}) error {
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

	origins := expandOrigins(d.Get("origin").(*schema.Set))

	removeOrigins(origins.Items, resp.DistributionConfig.Origins)

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

	return resourceAwsCloudFrontOriginRead(d, meta)
}

func removeOrigins(origins []*cloudfront.Origin, resp *cloudfront.Origins) {
	var qty int64
	var flat *schema.Set
	flat = flattenOrigins(resp)
	for _, v := range origins {
		if flat.Contains(flattenOrigin(v)) {
			flat.Remove(flattenOrigin(v))
			qty++
		}
	}
	expand := expandOrigins(flat)
	resp.SetItems(expand.Items)
	resp.SetQuantity(*resp.Quantity - qty)
}
