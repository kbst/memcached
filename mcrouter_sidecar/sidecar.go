package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"time"
)

type ConfigPool struct {
	Servers []string `json:"servers"`
}

type McRouterConfig struct {
	Pools map[string]ConfigPool `json:"pools"`
	Route string                `json:"route"`
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func get_config(addrs []net.IP) (config_json []byte, err error) {
	var servers []string
	for _, ip := range addrs {
		if ip.To4() != nil {
			servers = append(servers, fmt.Sprintf("%s:%s", ip, "11211"))
		}
	}
	sort.Strings(servers)

	pool_name := "default"

	config := McRouterConfig{
		Pools: map[string]ConfigPool{pool_name: {Servers: servers}},
		Route: fmt.Sprintf("PoolRoute|%s", pool_name),
	}

	config_json, err = json.Marshal(config)
	return
}

func main() {
	var debug bool
	flag.BoolVar(
		&debug,
		"debug",
		false,
		"Enable debug output.")
	var refresh time.Duration
	flag.DurationVar(
		&refresh,
		"refresh",
		time.Duration(5)*time.Second,
		"Specify time to wait between DNS queries.")
	var output string
	flag.StringVar(
		&output,
		"output",
		"mcrouter.conf",
		"Path to output file.")
	flag.Usage = func() {
		fmt.Printf("Usage: %s [OPTIONS] SVC_NAME\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	svc_name := flag.Arg(0)
	if svc_name == "" {
		flag.Usage()
		os.Exit(2)
	}

	for {
		addrs, err := net.LookupIP(svc_name)
		check(err)

		config_json, err := get_config(addrs)
		check(err)

		dat, _ := ioutil.ReadFile(output)
		if !bytes.Equal(dat, config_json) {
			err = ioutil.WriteFile(output, config_json, 0644)
			check(err)

			if debug == true {
				log.Printf(
					"Wrote '%s' to '%s'.", string(config_json[:]),
					output)
			}
		}

		time.Sleep(refresh)
	}
}
