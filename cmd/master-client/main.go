package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/hanfei1991/microcosom/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

func main() {
	cmd := os.Args[1]
	addr := ""
	switch cmd {
	case "submit-job":
		flag1 := os.Args[2]
		if flag1 != "--master-addr" {
			fmt.Printf("no master address found")
			os.Exit(1)
		}
		addr = os.Args[3]

	case "help":
		fmt.Printf("submit-job --config configFile")
		os.Exit(0)
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("err: %v", err)
	}
	clt := pb.NewMasterClient(conn)

	args := os.Args[4:]
	cfg := benchmark.NewConfig()
	err = cfg.Parse(args)
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		//common.PrintLinesf("parse cmd flags err: %s", terror.Message(err))
		fmt.Printf("err1: %v", err)
		os.Exit(2)
	}

	configJson, err := json.Marshal(cfg)
	if err != nil {
		fmt.Printf("err2: %v", err)
	}

	req := &pb.SubmitJobRequest{
		Tp:     pb.SubmitJobRequest_Benchmark,
		Config: configJson,
		User:   "hanfei",
	}

	resp, err := clt.SubmitJob(context.Background(), req)
	if err != nil {
		fmt.Printf("err: %v", err)
		return
	}
	if resp.ErrMessage != "" {
		fmt.Printf("err: %v", resp.ErrMessage)
		return
	}
	fmt.Print("submit job successful")
}
