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
	"rsync-os/rsync"
	"rsync-os/storage"
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

func ClientRclone(src, dest string) {
	addr, module, path, err := rsync.SplitURI(src)

	if err != nil {
		log.Println("Invaild URI")
		return
	}

	log.Println(module, path)

	ppath := rsync.TrimPrepath(path)
	fmt.Println(ppath)

	if viper.GetStringMapString(dest) == nil {
		log.Println("Lack of ", dest)
		return
	}

	// Config
	dbconf := viper.GetStringMapString(dest + ".boltdb")
	rcloneConf := viper.GetStringMapString(dest)
	fmt.Println(rcloneConf)

	// 打死硬编码
	rtor, _ := storage.NewRclone(rcloneConf["path"], "archlinuxcn/"+ppath, module, dbconf["path"])
	// rtor, _ := storage.NewRclone(rcloneConf["path"], "archlinux/"+ppath, module, dbconf["path"])
	defer rtor.Close()

	client, err := rsync.SocketClient(rtor, addr, module, ppath, nil)
	if err != nil {
		fmt.Println("SUPERERR    ", err)
		// panic("rsync client fails to initialize")
	}
	if err = client.Sync(); err != nil {
		fmt.Println("SUPERERR    ", err)
		// panic(err)
	}

	for err != nil {
		client, err = rsync.SocketClient(rtor, addr, module, ppath, nil)
		if err != nil {
			fmt.Println("SUPERERR    ", err)
			// panic("rsync client fails to initialize")
		}
		if err = client.Sync(); err != nil {
			fmt.Println("SUPERERR    ", err)
			// panic(err)
		}
	}
	fmt.Println("SUPERSUCCESS")

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
	// ClientS3(args[0], args[1])
	ClientRclone(args[0], args[1])
	log.Println("Duration:", time.Since(startTime))
}
