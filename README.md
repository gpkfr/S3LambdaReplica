# Compilation
```
GOOS=linux GOARCH=amd64 go build -ldflags "-w -s" -o bin/main && 
cd bin && 
zip function.zip main
```
# ENV
1. ```CONFIG_URL``` with an URL

2. ```CONFIG``` with Base64 Json config encoded

### Config json :


````
{
    "bucketNameSrc": {
        "region": "us-east-1",
        "destinations": ["bucketdest", "anotherbucket@eu-west-1"]
    },
    "bucketNamewithACLsrc": {
        "ACL": "public-read",
        "region": "us-east-1",
        "destinations": ["bucketnamedest@us-east-1"]
    }
}
```

- Note :

If you do not set the field "ACL", the function try to replicate the original acl.
