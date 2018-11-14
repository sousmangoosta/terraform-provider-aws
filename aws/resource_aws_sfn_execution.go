package aws

import (
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/sfn"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
)

func resourceAwsSfnExecution() *schema.Resource {
	return &schema.Resource{
		Create: resourceAwsSfnExecutionCreate,
		Read:   resourceAwsSfnExecutionRead,
		Delete: resourceAwsSfnExecutionDelete,

		Schema: map[string]*schema.Schema{
			"input": {
				Type:         schema.TypeString,
				Required:     false,
				ValidateFunc: validation.ValidateJsonString,
			},

			"name": {
				Type:         schema.TypeString,
				Required:     false,
				ValidateFunc: validateSfnExecutionName,
			},

			"state_machine_arn": {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validateArn,
			},

			"execution_arn": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"start_date": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"status": {
				Type:     schema.TypeString,
				Computed: true,
			},
		},
	}
}

func resourceAwsSfnExecutionCreate(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).sfnconn
	log.Print("[DEBUG] Executing Step Function State Machine")

	params := &sfn.StartExecutionInput{
		Input:           aws.String(d.Get("input").(string)),
		Name:            aws.String(d.Get("name").(string)),
		StateMachineArn: aws.String(d.Get("state_machine_arn").(string)),
	}

	var activity *sfn.StartExecutionOutput

	activity, err := conn.StartExecution(params)

	if err != nil {
		return fmt.Errorf("Error running Step Function Execution: %s", err)
	}

	d.SetId(*activity.ExecutionArn)

	return resourceAwsSfnExecutionRead(d, meta)
}

func resourceAwsSfnExecutionRead(d *schema.ResourceData, meta interface{}) error {
	conn := meta.(*AWSClient).sfnconn
	log.Printf("[DEBUG] Reading Step Function Execution: %s", d.Id())

	se, err := conn.DescribeExecution(&sfn.DescribeExecutionInput{
		ExecutionArn: aws.String(d.Id()),
	})
	if err != nil {

		if awserr, ok := err.(awserr.Error); ok {
			if awserr.Code() == "NotFoundException" || awserr.Code() == "ExecutionDoesNotExist" {
				d.SetId("")
				return nil
			}
		}
		return err
	}

	d.Set("input", se.Input)
	d.Set("name", se.Name)
	d.Set("status", se.Status)

	if err := d.Set("start_date", se.StartDate.Format(time.RFC3339)); err != nil {
		log.Printf("[DEBUG] Error setting start_date: %s", err)
	}

	return nil
}

func resourceAwsSfnExecutionDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[DEBUG] Deleting Step Function Execution: %s", d.Id())
	return nil
}
