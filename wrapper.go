/* 
@author: wangkai 
@contact: berryberryry@gmail.com
@version: 1.0 
@license: Apache Licence 
@file: wrapper.go
@time: 18-11-1 下午8:02 

这一行开始写关于本文件的说明与解释 
*/
package oss_wrapper

import (
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

func GetBucket(ossEndPoint string, ossAccessKeyId string, ossAccessKeySecret string, bucketName string) (*oss.Bucket, bool) {

	client, err := oss.New(ossEndPoint, ossAccessKeyId, ossAccessKeySecret)

	if err != nil {
		fmt.Println(err)
		return nil, false
	}

	bucket, err := client.Bucket(bucketName)
	if err != nil {
		fmt.Println(err)
		return nil, false
	}
	return bucket, true
}

func DownloadObjectFromOss(bucket *oss.Bucket, objectName string, outputFileName string) bool {

	// 判断文件是否存在。
	isExist, err := bucket.IsObjectExist(objectName)
	if err != nil {
		fmt.Println(err)
		return false
	}

	if isExist {

		// 下载文件。
		err := bucket.GetObjectToFile(objectName, outputFileName)
		if err != nil {
			fmt.Println(err)
		}
		return true
	}
	return false

}

func IsExist(bucket *oss.Bucket, objectName string) (bool, error) {
	isExist, err := bucket.IsObjectExist(objectName)
	return isExist, err
}

func ListObjects(bucket *oss.Bucket, marker string) ([]string, bool) {
	// 列举所有文件。
	objectsPaths := []string{}
	for {
		lsRes, err := bucket.ListObjects(oss.Marker(marker))
		if err != nil {
			fmt.Println(err)
			return nil, false
		}

		// 打印列举文件，默认情况下一次返回100条记录。
		for _, object := range lsRes.Objects {
			//fmt.Println("Bucket: ", object.Key)
			objectsPaths = append(objectsPaths, object.Key)
		}

		if lsRes.IsTruncated {
			marker = lsRes.NextMarker
		} else {
			break
		}
	}
	return objectsPaths, true
}
