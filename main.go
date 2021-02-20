// Send @RSYNCD x.x\n
// Send modname\n
// Send arugment with mod list\0	filter list write(0)    \n
// handshake
// batch seed
// Recv file list
//

package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"github.com/Si-Huan/rsync-os/rsync"
	"github.com/Si-Huan/rsync-os/storage"
	"time"

	"github.com/spf13/viper"
)

func ClientS3(src string, dest string) {
	addr, module, path, err := rsync.SplitURI(src)

	if err != nil {
		log.Println("Invaild URI")
		return
	}

	log.Println(module, path)

	ppath := rsync.TrimPrepath(path)

	if viper.GetStringMapString(dest) == nil {
		log.Println("Lack of ", dest)
		return
	}

	// Config
	dbconf := viper.GetStringMapString(dest + ".boltdb")
	minioConf := viper.GetStringMapString(dest)

	stor, _ := storage.NewMinio(module, ppath, dbconf["path"], minioConf["endpoint"], minioConf["keyaccess"], minioConf["keysecret"], false)
	defer stor.Close()

	client, err := rsync.SocketClient(stor, addr, module, ppath, nil)
	if err != nil {
		panic("rsync client fails to initialize")
	}
	if err := client.Sync(); err != nil {
		panic(err)
	}

}

func ClientTb(src string, dest string) {
	addr, module, path, err := rsync.SplitURI(src)

	if err != nil {
		log.Println("Invaild URI")
		return
	}

	log.Println(module, path)

	ppath := rsync.TrimPrepath(path)

	if viper.GetStringMapString(dest) == nil {
		log.Println("Lack of ", dest)
		return
	}

	// Config
	dbconf := viper.GetStringMapString(dest + ".boltdb")
	tbConf := viper.GetStringMapString(dest)

	log.Println(module, ppath)

	bucket := filepath.Join(tbConf["base"], module)

	stor, err := storage.NewTeambition(bucket, ppath, dbconf["path"], tbConf["cookie"])
	if err != nil {
		panic(err)
	}
	defer stor.Close()

	client, err := rsync.SocketClient(stor, addr, module, ppath, nil)
	if err != nil {
		panic("rsync client fails to initialize")
	}
	if err := client.Sync(); err != nil {
		panic(err)
	}
}

func main() {
	loadConfigIfExists()
	flag.Parse()
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Usage: rsync-os [OPTION]... rsync://[USER@]HOST[:PORT]/SRC")
		return
	}
	startTime := time.Now()
	//ClientS3(args[0], args[1])
	ClientTb(args[0], args[1])
	log.Println("Duration:", time.Since(startTime))
}
