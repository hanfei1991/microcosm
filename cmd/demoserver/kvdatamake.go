package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/pb"
	"google.golang.org/grpc"
)

const (
	FILENUM     = 3
	RECORDERNUM = 10
	FLUSHLEN    = 2
)

func main() {
	fmt.Println("Start to generate the data ...")
	generateData("./data", RECORDERNUM)
	fmt.Println("Listening the client call ..")
	DataService()

}

func generateData(folderName string, recorders int) int {
	_dir, err := ioutil.ReadDir(folderName)
	if err != nil {
		fmt.Printf("the folder %s doesn't exist ", folderName)
		return 0
	}
	for _, _file := range _dir {
		fileName := folderName + "/" + _file.Name()
		if _file.IsDir() {
			os.RemoveAll(fileName)
		} else {
			os.Remove(fileName)
		}
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	var fileNum int
	for fileNum = r.Intn(FILENUM); fileNum == 0; {
		fileNum = r.Intn(FILENUM)
	}
	fmt.Printf("the fold file number is  %d  ", fileNum)
	fileWriterMap := make(map[int]*bufio.Writer)
	for i := 0; i < fileNum; i++ {
		fileName := folderName + "/" + strconv.Itoa(i) + ".txt"
		file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
		if err != nil {
			fmt.Printf("make sure the file % s exist, the error is %v ", fileName, err)
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		fileWriterMap[i] = writer
	}
	shouldFlush := false
	for k := 0; k < recorders; k++ {
		if (k % FLUSHLEN) == 0 {
			shouldFlush = true
		}
		index := k % fileNum
		fileWriterMap[index].WriteString(strconv.Itoa(k) + "," + "value \r\n")
		if shouldFlush {
			fileWriterMap[index].Flush()
		}
		if index == 0 {
			shouldFlush = false
		}
	}
	for _, writer := range fileWriterMap {
		writer.Flush()
	}
	return fileNum
}

type DataRWServer struct {
	fileWriterMap map[string]*bufio.Writer
	fileReaderMap map[string]*bufio.Reader
}

func NewDataRWServer() *DataRWServer {
	s := &DataRWServer{
		fileReaderMap: make(map[string]*bufio.Reader),
		fileWriterMap: make(map[string]*bufio.Writer),
	}
	return s
}

func (*DataRWServer) ListFiles(ctx context.Context, folder *pb.ListFilesReq) (*pb.ListFilesResponse, error) {
	//fmt.Printf("receive the list file call %s", folder.String())
	fd := folder.FolderName
	_dir, err := ioutil.ReadDir(fd)
	fmt.Printf("receive the list file call %s \n", fd)
	if err != nil {
		return &pb.ListFilesResponse{}, err
	}

	files := []string{}
	for _, _file := range _dir {
		fileName := fd + "/" + _file.Name()
		if !_file.IsDir() {
			files = append(files, fileName)
		}
	}
	return &pb.ListFilesResponse{FileNames: files}, nil

}

func (s *DataRWServer) ReadLines(req *pb.ReadLinesRequest, stream pb.DataRWService_ReadLinesServer) error {
	fileName := req.FileName
	reader, exist := s.fileReaderMap[fileName]
	if !exist {
		file, err := os.OpenFile(fileName, os.O_RDONLY, 0o666)
		if err != nil {
			fmt.Printf("make sure the file % s exist, the error is %v ", fileName, err)
			return err
		}
		defer file.Close()
		reader = bufio.NewReader(file)

		s.fileReaderMap[fileName] = reader
		fmt.Println("read line ")
	} else {
		fmt.Println("the read already exist ")
	}
	fmt.Printf("enter the funct read line %v", fileName)
	i := 0
	for {
		reply, err := reader.ReadString('\n')
		if err == io.EOF {
			delete(s.fileReaderMap, fileName)
			fmt.Printf("the end of the file %v", fileName)
			break
		}
		reply = strings.TrimSpace(reply)
		fmt.Printf("the line no is %v , string is %v\n", i, reply)
		i++
		if reply == "" {
			continue
		}
		err = stream.Send(&pb.ReadLinesResponse{Linestr: reply})

		if err != nil {
			return err
		}
	}
	return nil

}

func (s *DataRWServer) WriteLines(stream pb.DataRWService_WriteLinesServer) error {
	for {
		defer func() {
			for _, writer := range s.fileWriterMap {
				writer.Flush()
			}
		}()
		count := 0
		res, err := stream.Recv()
		if err == nil {
			fileName := res.FileName
			//	fmt.Println("the string is ", fileName, res.Key, res.Value)
			writer, exist := s.fileWriterMap[fileName]
			if !exist {
				file, err1 := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0o666)
				if err1 != nil {
					fmt.Printf("make sure the file % s exist, the error is %v ", fileName, err1)
				}
				defer file.Close()
				writer = bufio.NewWriter(file)
				s.fileWriterMap[fileName] = writer
			}
			writer.WriteString(res.Key + "," + res.Value + "\n")
			fmt.Println(res)
			count++
			if (count % FLUSHLEN) == 0 {
				writer.Flush()
			}
		} else if err == io.EOF {
			return stream.SendAndClose(&pb.WriteLinesResponse{})
		}

		if err != nil {
			return err
		}

	}

}

func DataService() {
	grpcServer := grpc.NewServer()
	pb.RegisterDataRWServiceServer(grpcServer, NewDataRWServer())
	lis, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatal(err)
		fmt.Println("error happened ")
	}
	grpcServer.Serve(lis)
}
