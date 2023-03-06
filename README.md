# Notes

Testing the performance in memory used by lambda function, reading cloudfront log files from an s3 bucket, filtered by current day and whose uri is /. Using native python with loop, and then filtering unique values according to unique ip address consumes twice as much memory than running the same query with athena.
