package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io/fs"
	"log"
	"os"
	"strings"
)

var masterLogFile *os.File

func init() {
	err := os.Mkdir("./download", fs.ModePerm)
	if err != nil {
		log.Printf("error creating directory %v", err)
		if !strings.Contains(err.Error(), "exists") {
			os.Exit(0)
		}
	}
	err = os.Mkdir("./upload", fs.ModePerm)
	if err != nil {
		log.Printf("error creating directory %v", err)
		if !strings.Contains(err.Error(), "exists") {
			os.Exit(0)
		}
	}
	err = os.Mkdir("./logs", fs.ModePerm)
	if err != nil {
		log.Printf("error creating directory %v", err)
		if !strings.Contains(err.Error(), "exists") {
			os.Exit(0)
		}
	}
	file, _ := os.Create("./logs/master.log")
	masterLogFile = file
}


func main() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2")},
	)
	if err != nil {
		exitErrorf("error opening session")
	}

	svc := s3.New(sess)
	downloader := s3manager.NewDownloader(sess)
	uploader := s3manager.NewUploader(sess)
	var _ = uploader

	buckets := map[string]string{
		"dev" : "airbloc-warehouse-dev",
		"prod" : "airbloc-warehouse-prod",
	}
	locs := []string{"received-events", "full-events"}
	env := "dev"

	var fileCount = 0
	for _, loc := range locs {
		masterLogWriter := bufio.NewWriter(masterLogFile)

		resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket: aws.String(buckets[env]),
			Prefix: aws.String(loc),
		})
		if err != nil {
			errorMessage := fmt.Sprintf("Unable to list items in bucket %q, %v", buckets[env], err)
			masterLogWriter.Write([]byte(errorMessage))
			masterLogWriter.Flush()
			exitErrorf(errorMessage)
		}
		for _, item := range resp.Contents {
			key := *item.Key
			arr := strings.Split(*item.Key, "/")
			filename:= arr[len(arr) - 1]

			if filename != "" {
				fileCount++

				str := fmt.Sprintf("\nThis is file #%d\n", fileCount)
				masterLogWriter.Write([]byte(str))
				masterLogWriter.Flush()
				log.Print(str)

				Download(downloader, buckets[env], key, filename)
				RemoveJoongnaEvents(filename)
				Upload(uploader, buckets[env], key, filename)
				CleanUp(filename)

				masterLogWriter.Write([]byte{'\n'})
				masterLogWriter.Flush()
				fmt.Print("\n\n")
			}
		}
	}
	defer masterLogFile.Close()
}

func Download(downloader *s3manager.Downloader, bucket, key, filename string) {
	masterLogWriter := bufio.NewWriter(masterLogFile)
	defer masterLogWriter.Flush()

	file, err := os.Create(fmt.Sprintf("./download/%s", filename))
	if err != nil {
		errorMessage := fmt.Sprintf("error creating file for /download %v", err)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
		return
	}
	defer file.Close()

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to download item %q, %v", key, err)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
		return
	}

	summary := fmt.Sprintf("Downloaded %s: %d bytes\n", file.Name(), numBytes)
	masterLogWriter.Write([]byte(summary))
	log.Print(summary)
}

func RemoveJoongnaEvents(filename string) {
	masterLogWriter := bufio.NewWriter(masterLogFile)
	defer masterLogWriter.Flush()

	data, err := os.ReadFile(fmt.Sprintf("./download/%s", filename))
	if err != nil {
		errorMessage := fmt.Sprintf("error reading %s to fix", filename)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
		return
	}
	buffer := bytes.NewBuffer(data)

	deleteLogFileName := fmt.Sprintf("./logs/%s.delete.log", filename)
	deleteLogFile, err := os.Create(deleteLogFileName)
	if err != nil {
		errorMessage := fmt.Sprintf("error creating logfile for %s", filename)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
		return
	}

	deleteLogBuffer := bufio.NewWriter(deleteLogFile)

	gReader, err := gzip.NewReader(buffer)
	if err != nil {
		errorMessage := fmt.Sprintf("error calling new gzip reader for %s\n", filename)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
		return
	}

	newFile, err := os.Create(fmt.Sprintf("./upload/%s", filename))
	if err != nil {
		errorMessage := fmt.Sprintf("error creating filtered file for %s", filename)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
		return
	}
	defer newFile.Close()
	gw := gzip.NewWriter(newFile)

	var deletedLines int
	var lineData map[string]interface{}

	//action to perform (core logic)
	scanner := bufio.NewScanner(gReader)
	for scanner.Scan() {
		byteData := scanner.Bytes()
		err = json.Unmarshal(byteData, &lineData)
		if err != nil {
			errorMessage := fmt.Sprintf("error unmarshalling lineData")
			masterLogWriter.Write([]byte(errorMessage))
			exitErrorf(errorMessage)
			return
		}
		if lineData["dataSource"].(string) == "joongna" {
			deletedLines += 1
			_, err = deleteLogBuffer.Write(append(byteData, '\n'))
			if err != nil {
				errorMessage := fmt.Sprintf("error writing\n %s\n", string(byteData))
				masterLogWriter.Write([]byte(errorMessage))
				log.Fatalln(errorMessage)
				return
			}
		} else {
			_, err := gw.Write(append(byteData, '\n'))
			if err != nil {
				errorMessage := fmt.Sprintf("error writing\n %s to %s\n", string(byteData), newFile.Name())
				masterLogWriter.Write([]byte(errorMessage))
				log.Fatalln(errorMessage)
				return
			}
		}
	}
	err = gw.Close()
	if err != nil {
		exitErrorf("error closing to %s", newFile.Name())
		return
	}
	err = deleteLogBuffer.Flush()
	if err != nil {
		log.Fatalf("error flushing %s\n logfile", filename)
		return
	}
	defer func(deletedLines int) {
		deleteLogFile.Close()
		if deletedLines == 0 {
			os.Remove(deleteLogFileName)
		}
	}(deletedLines)

	summary := fmt.Sprintf("Deleted %d events from %s\n", deletedLines, filename)
	masterLogWriter.Write([]byte(summary))
	log.Print(summary)
}


func Upload(uploader *s3manager.Uploader, bucket, key, filename string, ) {
	masterLogWriter := bufio.NewWriter(masterLogFile)
	defer masterLogWriter.Flush()

	file, err := os.Open(fmt.Sprintf("./upload/%s", filename))
	defer file.Close()
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to open file %q, %v", err)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
	}
	defer file.Close()
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key: aws.String(key),
		Body: file,
	})
	if err != nil {
		errorMessage := fmt.Sprintf("Unable to upload %q to %q, %v", key, bucket, err)
		masterLogWriter.Write([]byte(errorMessage))
		exitErrorf(errorMessage)
	}
	summary := fmt.Sprintf("Successfully uploaded %q to %q\n", key, bucket)
	masterLogWriter.Write([]byte(summary))
	log.Print(summary)
}


func CleanUp(filename string) {
	paths := []string{"./download", "./upload"}
	for _, path := range paths {
		err := os.Remove(fmt.Sprintf("%s/%s", path, filename))
		if err != nil {
			log.Fatalf("error removing %s\n", filename)
		}
		fmt.Printf("removed %s\n", fmt.Sprintf("%s/%s", path, filename))
	}
}

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}