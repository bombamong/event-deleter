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
	"log"
	"os"
	"strings"
)

func exitErrorf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}



func main() {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("ap-northeast-2")},
	)
	if err != nil {
		exitErrorf("error opening session")
	}
	var _ = sess

	// test: received-events/2020/09/18/08/airbloc-events-6-2020-09-18-08-37-41-3fc9ff68-7249-490e-9fb6-762a75bde37d.gz
	svc := s3.New(sess)
	downloader := s3manager.NewDownloader(sess)

	buckets := map[string]*string{
		"dev" : aws.String("airbloc-warehouse-dev"),
		"prod" : aws.String("airbloc-warehouse-prod"),
	}

	env := "dev"
	resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: buckets[env],
		Prefix: aws.String("received-events"),
	})
	if err != nil {
		exitErrorf("Unable to list items in bucket %q, %v", buckets[env], err)
	}
	for _, item := range resp.Contents {
		v := ""
		v += fmt.Sprintf("Name:         %v \n", *item.Key)
		v += fmt.Sprintf("Last modified:%v \n", *item.LastModified)
		v += fmt.Sprintf("Size:         %v \n", *item.Size)
		v += fmt.Sprintf("Storage class:%v \n\n", *item.StorageClass)

		arr := strings.Split(*item.Key, "/")
		filename:= arr[len(arr) - 1]

		Download(downloader, *buckets[env], *item.Key, filename)
		RemoveJoongnaEvents(filename)

		fmt.Println(v)
		break
	}

	//for _, b := range result.Buckets {
		//_, err := w.WriteString(fmt.Sprintf("* %s created on %s\n",
		//	aws.StringValue(b.Name), aws.TimeValue(b.CreationDate)))
		//if err != nil {
		//	exitErrorf("error writing bucket info to file")
		//}



		//resp, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{Bucket: aws.String(aws.StringValue(b.Name))})
		//if err != nil {
		//	exitErrorf("Unable to list items in bucket %q, %v", aws.StringValue(b.Name), err)
		//}

		//for _, item := range resp.Contents {
		//	v := ""
		//	v += fmt.Sprintf("Name:         %v \n", *item.Key)
		//	v += fmt.Sprintf("Last modified:%v \n", *item.LastModified)
		//	v += fmt.Sprintf("Size:         %v \n", *item.Size)
		//	v += fmt.Sprintf("Storage class:%v \n\n", *item.StorageClass)
		//	fmt.Println(v)
		//	//_, err := w.WriteString(v)
		//	//if err != nil {
		//	//	exitErrorf("error writing item info to file")
		//	//}
		//}
	//}
	//err = w.Flush()
	//if err != nil {
	//	exitErrorf("error flushing writer")
	//}
}

func Download(downloader *s3manager.Downloader, bucket, key, filename string) {
	file, err := os.Create(fmt.Sprintf("./download/%s", filename))
	if err != nil {
		exitErrorf("error creating file for /download %v", err)
	}
	defer file.Close()

	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
	if err != nil {
		exitErrorf("Unable to download item %q, %v", key, err)
	}
	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
}

func RemoveJoongnaEvents(filename string) {
	data, err := os.ReadFile(fmt.Sprintf("./download/%s", filename))
	if err != nil {
		exitErrorf("error reading %s to fix", filename)
	}
	buffer := bytes.NewBuffer(data)

	logFile, err := os.Create(fmt.Sprintf("./output/%s.delete.log", filename))
	if err != nil {
		exitErrorf("error creating logfile for %s", filename)
	}
	defer logFile.Close()
	logBuffer := bufio.NewWriter(logFile)

	gReader, err := gzip.NewReader(buffer)
	if err != nil {
		exitErrorf("error calling new gzip reader for %s\n", filename)
	}

	newFile, err := os.Create(fmt.Sprintf("./filtered/%s", filename))
	if err != nil {
		exitErrorf("error creating filtered file for %s", filename)
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
			exitErrorf("error unmarshalling lineData")
		}
		if lineData["dataSource"].(string) == "joongna" {
			deletedLines += 1
			_, err = logBuffer.Write(append(byteData, '\n'))
			if err != nil {
				log.Fatalf("error writing\n %s\n", string(byteData))
			}
		} else {
			_, err := gw.Write(append(byteData, '\n'))
			if err != nil {
				log.Fatalf("error writing\n %s to %s\n", string(byteData), newFile.Name())
			}
		}
	}
	err = gw.Close()
	if err != nil {
		exitErrorf("error closing to %s", newFile.Name())
	}
	fmt.Printf("\nDeleted %d events from %s\n", deletedLines, filename)
	err = logBuffer.Flush()
	if err != nil {
		log.Fatalf("error flushing %s\n logfile", filename)
	}
}


func Upload(bucket, key, filepath string, uploader *s3manager.Uploader) {
	file, err := os.Open(filepath)
	if err != nil {
		exitErrorf("Unable to open file %q, %v", err)
	}
	defer file.Close()
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(bucket),
		Key: aws.String(key),
		Body: file,
	})
	if err != nil {
		// Print the error and exit.
		exitErrorf("Unable to upload %q to %q, %v", key, bucket, err)
	}
	fmt.Printf("Successfully uploaded %q to %q\n", key, bucket)
}


func CleanUp() {

}

// 파일 다운 -> save to zipped/
// unzip -> save to unzipped/
// open unzipped/*.json 파일 파싱 및 joongna 삭제 -> 지운 이벤트 filename.log 저장
// zip(GZIP) ->  upload/ 에 파일 저장
// original S3 파일 .erase 로 rename
// upload/*.gz 업로드
// rm donwload/* download/*
// repeat