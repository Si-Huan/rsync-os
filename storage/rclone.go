package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"rsync-os/rsync"
	"rsync-os/storage/cache"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	_ "github.com/rclone/rclone/backend/onedrive"
	rcfs "github.com/rclone/rclone/fs"
	rcobj "github.com/rclone/rclone/fs/object"
	bolt "go.etcd.io/bbolt"
)

type Rclone struct {
	client rcfs.Fs
	prefix string
	/* Cache */
	cache  *bolt.DB
	tx     *bolt.Tx
	bucket *bolt.Bucket
}

func NewRclone(drivePath string, prefix string, bucket string, cachePath string) (*Rclone, error) {
	client, err := rcfs.NewFs(context.Background(), drivePath)
	if err != nil {
		fmt.Println(err)
		panic("Failed to init a rclone client")
	}

	// Initialize cache
	db, err := bolt.Open(cachePath, 0666, nil)
	if err != nil {
		panic("Can't init cache: boltdb")
	}
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}

	// If bucket does not exist, create the bucket
	mod, err := tx.CreateBucketIfNotExists([]byte(bucket))
	if err != nil {
		return nil, err
	}

	return &Rclone{
		client: client,
		prefix: prefix,
		cache:  db,
		tx:     tx,
		bucket: mod,
	}, nil
}

func (r *Rclone) Put(fileName string, content io.Reader, fileSize int64, metadata rsync.FileMetadata) (written int64, err error) {
	fpath := filepath.Join(r.prefix, fileName)
	fsize := fileSize
	fname := fpath

	if metadata.Mode.IsDIR() {
		fsize = 0
	} else if metadata.Mode.IsLNK() {

	} else {
		fmt.Println("PUT   ", fileName)

		mtime := time.Unix(int64(metadata.Mtime), 0)
		src := rcobj.NewStaticObjectInfo(fname, mtime, fsize, true, nil, nil)
		writtenObj, err := r.client.Put(context.Background(), content, src)
		if err != nil {
			return -1, err
		}
		fsize = writtenObj.Size()
	}
	written = fsize
	value, err := proto.Marshal(&cache.FInfo{
		Size:  fileSize,
		Mtime: metadata.Mtime,
		Mode:  int32(metadata.Mode), // FIXME: convert uint32 to int32
	})
	if err != nil {
		return -1, err
	}
	if err := r.bucket.Put([]byte(fpath), value); err != nil {
		return -1, err
	}
	return
}

func (r *Rclone) Delete(fileName string, mode rsync.FileMode) (err error) {
	fpath := filepath.Join(r.prefix, fileName)
	if mode.IsDIR() {
		// err = r.client.Rmdir(context.Background(), fpath)
	} else if mode.IsLNK() {

	} else {
		obj, err := r.client.NewObject(context.Background(), fpath)
		if err != nil {
			return errors.New("Can't find object" + err.Error())
		}
		obj.Remove(context.Background())
		log.Println("RELDEL   ", fileName)

	}

	return r.bucket.Delete([]byte(fpath))
}

func (r *Rclone) List() (rsync.FileList, error) {
	filelist := make(rsync.FileList, 0, 1<<16)

	// We don't list all files directly

	info := &cache.FInfo{}

	// Add files in the work dir
	c := r.bucket.Cursor()
	prefix := []byte(r.prefix)
	k, v := c.Seek(prefix)
	hasdot := false
	for k != nil && bytes.HasPrefix(k, prefix) {
		p := k[len(prefix):]
		if bytes.Equal(p, []byte(".")) {
			hasdot = true
		}

		if err := proto.Unmarshal(v, info); err != nil {
			return filelist, err
		}
		filelist = append(filelist, rsync.FileInfo{
			Path:  p, // ignore prefix
			Size:  info.Size,
			Mtime: info.Mtime,
			Mode:  rsync.FileMode(info.Mode),
		})
		k, v = c.Next()
	}

	// Add current dir as .
	if !hasdot {
		workdir := []byte(filepath.Clean(r.prefix)) // If a empty string, we get "."
		v := r.bucket.Get(workdir)
		if v == nil {
			return filelist, nil
		}
		if err := proto.Unmarshal(v, info); err != nil {
			return filelist, err
		}
		filelist = append(filelist, rsync.FileInfo{
			Path:  []byte("."),
			Size:  info.Size,
			Mtime: info.Mtime,
			Mode:  rsync.FileMode(info.Mode),
		})
	}

	sort.Sort(filelist)

	return filelist, nil

	// var fileMode os.FileMode = 0664
	// fmod := rsync.FileMode(fileMode)
	// filelist := make(rsync.FileList, 0, 1<<16)

	// entries, err := r.client.List(context.Background(), "")
	// fmt.Println(err)
	// if err != nil {
	// 	return nil, err
	// }
	// for _, entry := range entries {
	// 	filelist = append(filelist, rsync.FileInfo{
	// 		Path:  []byte(entry.Remote()), // ignore prefix
	// 		Size:  entry.Size(),
	// 		Mtime: int32(entry.ModTime(context.Background()).Unix()),
	// 		Mode:  fmod,
	// 	})
	// }
	// sort.Sort(filelist)

	// return filelist, nil
}

func (r *Rclone) Close() error {
	defer r.cache.Close()
	if err := r.tx.Commit(); err != nil {
		return err
	}
	return nil
}
