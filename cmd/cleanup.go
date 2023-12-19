package cmd

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/manifoldco/promptui"
	"github.com/spf13/cobra"
)

// cleanupCmd represents the cleanup command
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup lingering s3 buckets",
	Long:  `Command line utility to cleanup s3 buckets created from development efforts.`,
	Run: func(cmd *cobra.Command, args []string) {
	},
}

func init() {
	rootCmd.AddCommand(cleanupCmd)

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	svc := s3.New(sess)

	buckets, err := listBuckets(svc)

	if err != nil {
		rootCmd.PrintErr("Error in listing buckets:", err)
		return
	}

	fmt.Println("Select a bucket to delete:")
	bucketNames := convertBucketListToStrings(buckets)

	prompt := promptui.Select{
		Label: "Select bucket to delete",
		Items: bucketNames,
	}

	_, result, err := prompt.Run()

	if err != nil {
		fmt.Printf("Prompt failed %v\n", err)
		return
	}

	confirm := promptui.Prompt{
		Label:     fmt.Sprintf("Are you sure you want to delete the bucket '%s' (yes/no)", result),
		IsConfirm: true,
		Validate: func(input string) error {
			if strings.ToLower(input) != "yes" && strings.ToLower(input) != "no" {
				return errors.New("please enter 'yes' or 'no'")
			}
			return nil
		},
	}

	confirmResult, err := confirm.Run()

	if strings.ToLower(confirmResult) != "yes" {
		fmt.Println("Bucket deletion cancelled.")
		return
	}

	// We need to get the region of the bucket and create a new session with that region
	// in order to delete the bucket
	bucketRegionResult, err := svc.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(result),
	})
	if err != nil {
		fmt.Printf("Unable to get bucket location: %s\n", err)
		return
	}

	region := aws.StringValue(bucketRegionResult.LocationConstraint)

	fmt.Printf("Now deleting: %s\n", result)

	deleteSession := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String(region),
		},
	}))

	deleteService := s3.New(deleteSession)
	err = emptyBucket(deleteService, &result)

	if err != nil {
		rootCmd.PrintErr("Error in emptying bucket:", err)
		return
	}

	err = deleteBucket(deleteService, &result)

	if err != nil {
		rootCmd.PrintErr("Error in deleting bucket:", err)
		return
	}

	fmt.Printf("Successfully deleted bucket: %s\n", result)
}

func listBuckets(svc *s3.S3) ([]*s3.Bucket, error) {
	result, err := svc.ListBuckets(nil)
	if err != nil {
		return nil, err
	}
	return result.Buckets, nil
}

func convertBucketListToStrings(buckets []*s3.Bucket) []string {
	var bucketNames []string
	for _, bucket := range buckets {
		if bucket.Name != nil {
			bucketNames = append(bucketNames, *bucket.Name)
		}
	}
	return bucketNames
}

func emptyBucket(svc *s3.S3, bucketName *string) error {

	// List all object versions
	err := svc.ListObjectVersionsPages(&s3.ListObjectVersionsInput{
		Bucket: bucketName,
	}, func(page *s3.ListObjectVersionsOutput, lastPage bool) bool {
		// Delete each version
		for _, version := range page.Versions {
			fmt.Printf("\rDeleting version: %s\n", *version.Key)
			_, err := svc.DeleteObject(&s3.DeleteObjectInput{
				Bucket:    bucketName,
				Key:       version.Key,
				VersionId: version.VersionId,
			})
			if err != nil {
				return false
			}
		}

		// Delete each delete marker
		for _, marker := range page.DeleteMarkers {
			fmt.Printf("\rDeleting delete marker: %s\n", *marker.Key)
			_, err := svc.DeleteObject(&s3.DeleteObjectInput{
				Bucket:    bucketName,
				Key:       marker.Key,
				VersionId: marker.VersionId,
			})
			if err != nil {
				return false
			}
		}

		return !lastPage
	})

	if err != nil {
		return err
	}

	// Now delete all current objects using your existing logic
	iter := s3manager.NewDeleteListIterator(svc, &s3.ListObjectsInput{
		Bucket: bucketName,
	})

	batcher := s3manager.NewBatchDeleteWithClient(svc)
	if err := batcher.Delete(aws.BackgroundContext(), iter); err != nil {
		return err
	}

	return nil
}

func deleteBucket(svc *s3.S3, bucketName *string) error {
	// Delete the bucket
	_, err := svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(*bucketName),
	})
	return err
}
