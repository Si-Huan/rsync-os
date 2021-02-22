package storage

import (
	"bytes"
	"context"
	"errors"
	"github.com/Si-Huan/rsync-os/rsync"
	"github.com/Si-Huan/rsync-os/storage/cache"
	"io"
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	tba "github.com/Si-Huan/teambition-pan-api"
	"github.com/golang/protobuf/proto"
	bolt "go.etcd.io/bbolt"
)

var NotFound = errors.New("not found key in db")
var TargetNotFound = errors.New("link target not found")
var NotFile = errors.New("it is a dir")

type Teambition struct {
	client     tba.Fs
	bucketName string
	prefix     string
	ctx        context.Context
	/* Cache */
	cache  *bolt.DB
	tx     *bolt.Tx
	bucket *bolt.Bucket

	mutex *sync.Mutex
}

func NewTeambition(bucket string, prefix string, cachePath string, cookie string) (*Teambition, error) {
	config := &tba.Config{
		Cookie: cookie,
	}
	ctx := context.Background()
	teambitionClient, err := tba.NewFs(ctx, config)
	if err != nil {
		panic("Failed to init a teambition client")
	}

	// Create workplace
	bucketNode, err := teambitionClient.CreateFolder(ctx, filepath.Join(bucket, prefix))
	if err != nil {
		log.Fatalln(err)
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

	value, err := proto.Marshal(&cache.TbFInfo{
		Size:  0,
		Mtime: 0,
		Mode:  16877,
		ObjId: []byte(bucketNode.NodeId),
	})
	if err != nil {
		return nil, err
	}

	if err := mod.Put([]byte(filepath.Clean(prefix)), value); err != nil {
		return nil, err
	}

	mutex := new(sync.Mutex)
	return &Teambition{
		client:     teambitionClient,
		bucketName: bucket,
		prefix:     prefix,
		ctx:        ctx,
		cache:      db,
		tx:         tx,
		bucket:     mod,
		mutex:      mutex,
	}, nil
}

// object can be a regualar file, folder or symlink
func (tb *Teambition) Put(fileName string, content io.Reader, fileSize int64, metadata rsync.FileMetadata) (written int64, err error) {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	var (
		parentPath, name string
		objid            []byte
		parentNode       *tba.Node
	)
	fpath := filepath.Join(tb.prefix, fileName)
	i := strings.LastIndex(fpath, "/")
	if i == -1 {
		parentPath = "."
		name = fpath
	} else {
		parentPath = fpath[:i]
		name = fpath[i+1:]
	}

	if metadata.Mode.IsLNK() {
		relativePath, err := ioutil.ReadAll(content)
		if err != nil {
			return -1, err
		}
		objid = []byte(path.Join(parentPath, string(relativePath)))
	} else {
		selfNode, err := tb.node(fpath)
		if err != nil && err != NotFound {
			return -1, err
		}

		if metadata.Mode.IsREG() {
			parentNode, err = tb.node(parentPath)
			if err != nil {
				return -1, errors.New("no parent")
			}

			if selfNode != nil {
				err = tb.client.Delete(tb.ctx, selfNode)
				if err != nil {
					return -1, err
				}
			}

			newNode, err := tb.client.CreateFileIn(tb.ctx, parentNode, name, fileSize, content, false)
			if err != nil {
				return -1, err
			}
			objid = []byte(newNode.NodeId)
		} else if metadata.Mode.IsDIR() {
			if selfNode != nil {
				objid = []byte(selfNode.NodeId)
			} else {
				parentNode, err = tb.node(parentPath)
				if err != nil {
					return -1, errors.New("no parent")
				}

				newNode, err := tb.client.CreateFolderIn(tb.ctx, parentNode, name)
				if err != nil {
					return -1, err
				}
				objid = []byte(newNode.NodeId)
			}
		}
	}

	value, err := proto.Marshal(&cache.TbFInfo{
		Size:  fileSize,
		Mtime: metadata.Mtime,
		Mode:  int32(metadata.Mode), // FIXME: convert uint32 to int32
		ObjId: objid,
	})
	if err != nil {
		return -1, err
	}
	if err := tb.bucket.Put([]byte(fpath), value); err != nil {
		return -1, err
	}
	return 0, nil
}

func (tb *Teambition) Delete(fileName string, mode rsync.FileMode) (err error) {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	fpath := filepath.Join(tb.prefix, fileName)

	if !mode.IsLNK() {
		deleteNode, err := tb.node(fpath)
		if err != nil {
			return err
		}
		if err = tb.client.Delete(tb.ctx, deleteNode); err != nil {
			return err
		}

	}

	return tb.bucket.Delete([]byte(fpath))
}

// EXPERIMENTAL
func (tb *Teambition) List() (rsync.FileList, error) {
	filelist := make(rsync.FileList, 0, 1<<16)

	// We don't list all files directly

	info := &cache.TbFInfo{}

	// Add files in the work dir
	c := tb.bucket.Cursor()
	prefix := []byte(tb.prefix)
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
		workdir := []byte(filepath.Clean(tb.prefix)) // If a empty string, we get "."
		v := tb.bucket.Get(workdir)
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
}

func (tb *Teambition) FinishSync() (err error) {
	//Commit
	if err := tb.tx.Commit(); err != nil {
		if err := tb.tx.Rollback(); err != nil {
			return err
		}
		return err
	}

	//New tx
	if tb.tx, err = tb.cache.Begin(true); err != nil {
		return err
	}
	tb.bucket = tb.tx.Bucket([]byte(tb.bucketName))
	if tb.bucket == nil {
		return errors.New("blot get bucket err")
	}
	return nil
}

func (tb *Teambition) Close() error {
	tb.mutex.Lock()
	defer tb.mutex.Unlock()

	defer func() {
		if err := tb.cache.Close(); err != nil {
			log.Println(err)
		}
	}()
	if err := tb.tx.Commit(); err != nil {
		if err := tb.tx.Rollback(); err != nil {
			return err
		}
		return err
	}
	return nil
}

func (tb *Teambition) node(path string) (*tba.Node, error) {
	v := tb.bucket.Get([]byte(path))
	if v == nil {
		return nil, NotFound
	}
	nodeInfo := &cache.TbFInfo{}
	if err := proto.Unmarshal(v, nodeInfo); err != nil {
		return nil, err
	}
	if rsync.FileMode(nodeInfo.Mode).IsLNK() {
		v := tb.bucket.Get(nodeInfo.ObjId)
		if v == nil {
			return nil, TargetNotFound
		}
		if err := proto.Unmarshal(v, nodeInfo); err != nil {
			return nil, err
		}

	}
	return &tba.Node{NodeId: string(nodeInfo.ObjId)}, nil
}

//not allow dir path
func (tb *Teambition) GetURI(fileName string) (uri string, err error) {
	fpath := filepath.Join(tb.prefix, fileName)
	node, err := tb.node(fpath)
	if err != nil {
		return "", err
	}
	node, err = tb.client.GetbyNodeId(tb.ctx, node.NodeId)
	if err != nil {
		return "", err
	}
	if node.DownloadUrl == "" {
		return "", NotFile
	}

	return node.DownloadUrl, nil
}
