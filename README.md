```sh
Usage of ./remove-delete-markers:
  -access-key string
    	S3 Access Key
  -bucket string
    	Select a specific bucket
  -bypass-governance
    	Bypass governance on deletion
  -endpoint string
    	S3 endpoint URL
  -insecure
    	Disable TLS verification
  -prefix string
    	Select an object/prefix
  -remote-access-key string
    	S3 Access Key of the remote target
  -remote-endpoint string
    	S3 endpoint URL of the remote target
  -remote-secret-key string
    	S3 Secret Key of the remote target
  -secret-key string
    	S3 Secret Key
```

Example :-

```sh
./remove-delete-markers --endpoint https://192.168.0.119:30698 --access-key <access> --secret-key <secret> --bucket bucket1 --prefix /prefixb --insecure --remote-endpoint <remote-endpoint> --remote-access-key <remote-access> --remote-secret-key <remote-secret>
```
