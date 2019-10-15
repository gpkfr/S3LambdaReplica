#Compilation
```
GOOS=linux GOARCH=amd64 go build -ldflags "-w -s" -o bin/main && 
cd bin && 
zip function.zip main
```

