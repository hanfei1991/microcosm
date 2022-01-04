package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"google.golang.org/grpc"
)

const (
	ADDRESS    = "127.0.0.1:1234"
	BUFFERSIZE = 5
)

type kv struct {
	key   string
	value string
}

type democlient struct {
	cli    pb.DataRWServiceClient
	buffer chan kv
}

func NewDemoclient(serverAddr string) (*democlient, error) {
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("init the client  failed %v", err)
		return &democlient{nil, nil}, err
	}
	//defer conn.Close()
	buf := make(chan kv, BUFFERSIZE)
	client := pb.NewDataRWServiceClient(conn)
	demo := &democlient{cli: client, buffer: buf}
	return demo, nil

}

func (c *democlient) GetFilesList(ctx context.Context, sources string) ([]string, error) {
	if c.cli == nil {
		fmt.Printf("the client inited failed")
		return []string{}, nil

	}
	reply, err := (c.cli).ListFiles(ctx, &pb.ListFilesReq{FolderName: sources})
	if err != nil {
		fmt.Printf("error happened are %v", err)
		return []string{}, err
	}
	fmt.Printf("the files name are %v", reply.String())
	return reply.GetFileNames(), nil
}

func (c *democlient) Receive(ctx context.Context, sources string) error {
	reader, err := (c.cli).ReadLines(ctx, &pb.ReadLinesRequest{FileName: sources})
	if err != nil {
		fmt.Printf("receive funct failed %v", err)
		return err
	}
	fmt.Printf("the files name are %v", sources)
	for {
		linestr, err := reader.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		fmt.Printf("read the string %v \n", linestr.Linestr)
		strs := strings.Split(linestr.Linestr, ",")
		if len(strs) >= 2 {
			c.buffer <- kv{key: strs[0], value: strs[1]}
		} else {
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (c *democlient) Send(ctx context.Context, dest string) error {
	writer, err := (c.cli).WriteLines(ctx)
	if err != nil {
		fmt.Printf("send funct failed %v", err)
		return err
	}
	for {
		select {
		case kv := <-c.buffer:
			if err := writer.Send(&pb.WriteLinesRequest{FileName: dest, Key: kv.key, Value: kv.value}); err != nil {
				log.Fatal(err)
			}
		default:
			time.Sleep(time.Second)
		}
	}

}

func main() {
	client, err := NewDemoclient(ADDRESS)
	if err != nil {
		fmt.Printf("error happened %v", err)
		return
	}
	files, err := client.GetFilesList(context.Background(), "./data")
	if (err != nil) || len(files) == 0 {
		fmt.Printf("no file found %v", err)
		return
	}
	firstfile := strings.Split(files[0], "/")
	fileName := "./data2/" + firstfile[len(firstfile)-1]
	go client.Receive(context.Background(), files[0])
	go client.Send(context.Background(), fileName)
	for {
		fmt.Printf("the size of chan %v\n", len(client.buffer))
		time.Sleep(time.Second)
	}

}

/*
func main() {
	conn, err := grpc.Dial(ADDRESS, grpc.WithInsecure())
	if err != nil {
		return
	}
	defer conn.Close()
	client := pb.NewDataRWServiceClient(conn)
	reply, err := client.ListFiles(context.Background(), &pb.ListFilesReq{FolderName: "./data"})
	if err != nil {
		fmt.Println("error happened when call the list files method ", err)
		return
	}
	files := reply.GetFileNames()
	for _, file := range files {
		fmt.Printf("file name is %s", file)
	}
	reader, err1 := client.ReadLines(context.Background(), &pb.ReadLinesRequest{FileName: files[0]})
	if err1 != nil {
		log.Fatal(err1)
	}
	writer, err := client.WriteLines(context.Background())
	for {
		linestr, err := reader.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		strs := strings.Split(linestr.Linestr, ",")
		if len(strs) > 1 {
			fmt.Printf("key is %s,value is %s ", strs[0], strs[1])
			if err := writer.Send(&pb.WriteLinesRequest{FileName: "./data1/0.txt", Key: strs[0], Value: strs[1]}); err != nil {
				log.Fatal(err)
			}
			time.Sleep(time.Second)
		}

	}

}*/
