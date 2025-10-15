package namespacemanager

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/caleberi/distributed-system/common"
	"github.com/caleberi/distributed-system/utils"
)

// SerializedNsTreeNode represents a serialized node in the namespace tree, used for persistence.
type SerializedNsTreeNode struct {
	IsDir    bool           // Indicates if the node is a directory
	Path     common.Path    // The path of the node
	Children map[string]int // Map of child node names to their indices in the serialized slice
	Chunks   int64          // Number of chunks for file nodes
}

// NsTree represents a node in the namespace tree, which can be a file or directory.
type NsTree struct {
	sync.RWMutex             // Embedded mutex for thread-safe access
	Path         common.Path // The path of the node

	// File-related fields
	Length int64 // Length of the file (for file nodes)
	Chunks int64 // Number of chunks in the file (for file nodes)

	// Directory-related fields
	IsDir         bool               // Indicates if the node is a directory
	childrenNodes map[string]*NsTree // Map of child nodes (for directory nodes)
}

// NamespaceManager manages the namespace tree for a distributed file system.
type NamespaceManager struct {
	root                 *NsTree             // Root of the namespace tree
	serializationCount   int                 // Counter for serialization operations
	deserializationCount int                 // Counter for deserialization operations
	cleanUpInterval      time.Duration       // Interval for periodic cleanup of deleted nodes
	deleteCache          map[string]struct{} // Cache for tracking deleted nodes
	mu                   sync.Mutex
	ctx                  context.Context
	cancel               context.CancelFunc
	cleanupChan          chan string
}

// NewNameSpaceManager creates a new NamespaceManager with the specified cleanup interval.
func NewNameSpaceManager(ctx context.Context, cleanUpInterval time.Duration) *NamespaceManager {
	nctx, cancelFunc := context.WithCancel(ctx)
	nm := &NamespaceManager{
		ctx:    nctx,
		cancel: cancelFunc,
		root: &NsTree{
			IsDir:         true,
			Path:          "*",
			childrenNodes: make(map[string]*NsTree),
		},
		cleanUpInterval: cleanUpInterval,
		deleteCache:     make(map[string]struct{}),
		cleanupChan:     make(chan string, 100), // FIX: Initialize the channel
	}

	go func(nm *NamespaceManager) {
		for {
			select {
			case <-nm.ctx.Done():
				return
			case path := <-nm.cleanupChan:
				dirpath, filename := nm.RetrievePartitionFromPath(common.Path(path))
				parents, cwd, err := nm.lockParents(common.Path(dirpath), true)
				if err != nil {
					continue
				}
				delete(cwd.childrenNodes, filename)
				nm.unlockParents(parents, true)

				nm.mu.Lock()
				delete(nm.deleteCache, path)
				nm.mu.Unlock()
			}
		}
	}(nm)

	go func(nm *NamespaceManager) {
		for {
			select {
			case <-nm.ctx.Done():
				return
			case <-time.After(cleanUpInterval):
				nm.mu.Lock()
				for path := range nm.deleteCache {
					select {
					case nm.cleanupChan <- path:
					default:
					}
				}
				nm.mu.Unlock()
			}
		}
	}(nm)

	return nm
}

// SliceToNsTree converts a slice of serialized nodes into an NsTree, starting from the specified index.
func (nm *NamespaceManager) SliceToNsTree(r []SerializedNsTreeNode, id int) *NsTree {
	n := &NsTree{
		Path:   r[id].Path,
		Chunks: r[id].Chunks,
		IsDir:  r[id].IsDir,
	}
	if r[id].IsDir {
		n.childrenNodes = make(map[string]*NsTree)
		for k, child := range r[id].Children {
			n.childrenNodes[k] = nm.SliceToNsTree(r, child)
		}
	}
	nm.deserializationCount++
	return n
}

// Deserialize reconstructs the namespace tree from a slice of serialized nodes.
func (nm *NamespaceManager) Deserialize(nodes []SerializedNsTreeNode) *NsTree {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.root.RLock()
	defer nm.root.RUnlock()
	nm.root = nm.SliceToNsTree(nodes, len(nodes)-1)
	return nm.root
}

// Serialize converts the namespace tree into a slice of SerializedNsTreeNode for persistence.
func (nm *NamespaceManager) Serialize() []SerializedNsTreeNode {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	nm.root.RLock()
	defer nm.root.RUnlock()

	nm.serializationCount = 0
	ret := []SerializedNsTreeNode{}
	nm.nsTreeToSlice(&ret, nm.root)
	return ret
}

// nsTreeToSlice recursively converts an NsTree node into a SerializedNsTreeNode and appends it to the result slice.
func (nm *NamespaceManager) nsTreeToSlice(r *[]SerializedNsTreeNode, node *NsTree) int {
	n := SerializedNsTreeNode{
		IsDir:  node.IsDir,
		Chunks: node.Chunks,
		Path:   node.Path,
	}
	if n.IsDir {
		n.Children = make(map[string]int)
		for k, child := range node.childrenNodes {
			n.Children[k] = nm.nsTreeToSlice(r, child)
		}
	}
	*r = append(*r, n)
	ret := nm.serializationCount
	nm.serializationCount++
	return ret
}

// lockParents acquires locks on the ancestor nodes up to (and including) the parent directory of the given path.
func (nm *NamespaceManager) lockParents(p common.Path, lock bool) ([]*NsTree, *NsTree, error) {
	log.Printf("path: >>> %s", p)
	locked := []*NsTree{}
	current := nm.root
	nm.root.RLock()
	locked = append(locked, nm.root)

	if string(p) == "" || string(p) == "/" {
		if lock {
			nm.root.RUnlock()
			nm.root.Lock()
		}
		return locked, nm.root, nil
	}

	pf := strings.Split(string(p), "/")
	parts := []string{}
	for _, part := range pf {
		if part != "" {
			parts = append(parts, part)
		}
	}

	for i, name := range parts {
		child, ok := current.childrenNodes[name]
		if !ok {
			for j := len(locked) - 1; j >= 0; j-- {
				locked[j].RUnlock()
			}
			return nil, nil, fmt.Errorf("path %s is not found", p)
		}

		current.RUnlock()
		locked = locked[:len(locked)-1]

		isLast := (i == len(parts)-1)
		if isLast && lock {
			child.Lock()
		} else {
			child.RLock()
		}
		locked = append(locked, child)
		current = child
	}
	return locked, current, nil
}

// unlockParents releases the locks on the provided list of locked nodes in reverse order.
func (nm *NamespaceManager) unlockParents(locked []*NsTree, lock bool) {
	if len(locked) == 0 {
		return
	}
	lastIndex := len(locked) - 1
	for i := lastIndex; i >= 0; i-- {
		if i == lastIndex && lock {
			locked[i].Unlock()
		} else {
			locked[i].RUnlock()
		}
	}
}

// Create adds a new file node to the namespace at the specified path.
func (nm *NamespaceManager) Create(p common.Path) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	var (
		filename string
		dirpath  string
	)
	dirpath, filename = nm.RetrievePartitionFromPath(p)
	err := utils.ValidateFilename(filename, p)
	if err != nil {
		return err
	}

	parents, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(parents, true)
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[filename]; ok {
		return nil
	}

	cwd.childrenNodes[filename] = &NsTree{Path: p, IsDir: false}
	return nil
}

// Delete marks a file or directory at the specified path as deleted by prefixing its name.
func (nm *NamespaceManager) Delete(p common.Path) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	dirpath, filename := nm.RetrievePartitionFromPath(p)
	err := utils.ValidateFilename(filename, p)
	if err != nil {
		return err
	}

	parents, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(parents, true)
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[filename]; !ok {
		return fmt.Errorf("path does not %s exist", p)
	}

	node := cwd.childrenNodes[filename]
	deletedNodeKey := fmt.Sprintf("%s%s", common.DeletedNamespaceFilePrefix, filename)
	node.Path = common.Path(deletedNodeKey)
	cwd.childrenNodes[deletedNodeKey] = node
	delete(cwd.childrenNodes, filename)

	nm.deleteCache[string(p)] = struct{}{}

	return nil
}

// MkDir creates a new directory node at the specified path if it does not already exist.
func (nm *NamespaceManager) MkDir(p common.Path) error {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	var (
		dirpath string
		dirname string
	)
	dirpath, dirname = nm.RetrievePartitionFromPath(p)
	parents, cwd, err := nm.lockParents(common.Path(dirpath), true)
	defer nm.unlockParents(parents, true)
	if err != nil {
		return err
	}

	if _, ok := cwd.childrenNodes[dirname]; ok {
		return nil
	}
	cwd.childrenNodes[dirname] = &NsTree{
		IsDir:         true,
		childrenNodes: make(map[string]*NsTree),
		Path:          p,
	}
	return nil
}

// MkDirAll creates all necessary parent directories for the specified path, similar to `mkdir -p`.
// It ensures the entire path exists by creating directories as needed, but skips the final segment
// if it resembles a file (has a common extension) to prevent misclassifying files as directories.
// If the full path resembles a file, it creates directories only up to the parent.
// MkDirAll creates all necessary parent directories for the specified path, similar to `mkdir -p`.
// It ensures the entire path exists by creating directories as needed, but skips the final segment
// if it resembles a file (has a common extension) to prevent misclassifying files as directories.
// If the full path resembles a file, it creates directories only up to the parent.
// This implementation is iterative to avoid recursion depth issues.
func (nm *NamespaceManager) MkDirAll(p common.Path) error {
	if string(p) == "" || string(p) == "/" {
		return nil
	}

	fullPath := string(p)
	log.Printf("fullPath:%s", fullPath)
	dirPath, lastSegment := nm.RetrievePartitionFromPath(p)
	targetPathStr := fullPath
	if filepath.Ext(lastSegment) != "" {
		if dirPath == "" {
			return nil
		}
		targetPathStr = dirPath
	}
	segments := strings.Split(targetPathStr, "/")
	currentPath := "/"
	for _, seg := range segments {
		if seg == "" {
			continue
		}
		nextPath := filepath.Join(currentPath, seg)
		currP := common.Path(nextPath)

		// if _, err := nm.Get(currP); err == nil {
		// 	currentPath = nextPath
		// 	continue
		// }

		if err := nm.MkDir(currP); err != nil {
			return err
		}
		currentPath = nextPath
	}

	log.Printf("currentPath:%s", currentPath)
	log.Printf("Path:%s ?>", targetPathStr)
	return nil
}

// Get retrieves the NsTree node at the specified path, returning an error if it does not exist.
func (nm *NamespaceManager) Get(p common.Path) (*NsTree, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	dirpath, filenameOrDirname := nm.RetrievePartitionFromPath(p)
	parents, cwd, err := nm.lockParents(common.Path(dirpath), false)
	defer nm.unlockParents(parents, false)
	if err != nil {
		log.Println("SHITT")
		return nil, err
	}
	if dir, ok := cwd.childrenNodes[filenameOrDirname]; ok {
		log.Println("NICE")
		return dir, nil
	}
	return nil, fmt.Errorf("node with path %s does not exist for %s", dirpath, filenameOrDirname)
}

// Rename moves a file or directory from the source path to the target path.
func (nm *NamespaceManager) Rename(source, target common.Path) error {
	nm.mu.Lock()
	if source == target {
		return nil
	}
	err := errors.Join(
		utils.ValidateFilename(string(source), source),
		utils.ValidateFilename(string(target), target))
	if err != nil {
		nm.mu.Unlock()
		return fmt.Errorf("invalid path: %v", err)
	}

	srcDirpath, srcName := nm.RetrievePartitionFromPath(source)
	tgtDirpath, tgtName := nm.RetrievePartitionFromPath(target)
	nm.mu.Unlock()

	if _, err := nm.Get(common.Path(source)); err != nil {
		return fmt.Errorf("failed to locate source directory: %v", err)
	}

	if tgtDirpath != "" {
		if err := nm.MkDirAll(common.Path(tgtDirpath)); err != nil {
			return fmt.Errorf("failed to create target directory: %v", err)
		}
	}

	log.Println("==================++YESS=====")

	nm.mu.Lock()
	defer nm.mu.Unlock()

	if srcDirpath == tgtDirpath {
		// Same parent directory - only need to lock once
		parents, parent, err := nm.lockParents(common.Path(srcDirpath), true)
		if err != nil {
			return err
		}
		defer nm.unlockParents(parents, true)

		srcNode, ok := parent.childrenNodes[srcName]
		if !ok {
			return fmt.Errorf("source path %s does not exist", source)
		}

		if _, exists := parent.childrenNodes[tgtName]; exists {
			return fmt.Errorf("target path %s already exists", target)
		}

		delete(parent.childrenNodes, srcName)
		srcNode.Path = target
		parent.childrenNodes[tgtName] = srcNode

		return nil
	}

	lockSrcFirst := srcDirpath < tgtDirpath

	var srcParentsLocked, tgtParentsLocked []*NsTree
	var srcParentNode, tgtParentNode *NsTree

	if lockSrcFirst {
		srcParentsLocked, srcParentNode, err = nm.lockParents(common.Path(srcDirpath), true)
		if err != nil {
			return err
		}
		defer nm.unlockParents(srcParentsLocked, true)

		tgtParentsLocked, tgtParentNode, err = nm.lockParents(common.Path(tgtDirpath), true)
		if err != nil {
			return err
		}
		defer nm.unlockParents(tgtParentsLocked, true)
	} else {
		tgtParentsLocked, tgtParentNode, err = nm.lockParents(common.Path(tgtDirpath), true)
		if err != nil {
			return err
		}
		defer nm.unlockParents(tgtParentsLocked, true)

		srcParentsLocked, srcParentNode, err = nm.lockParents(common.Path(srcDirpath), true)
		if err != nil {
			return err
		}
		defer nm.unlockParents(srcParentsLocked, true)
	}

	if _, ok := tgtParentNode.childrenNodes[tgtName]; ok {
		return fmt.Errorf("target path %s already exists", target)
	}

	srcNode, ok := srcParentNode.childrenNodes[srcName]
	if !ok {
		return fmt.Errorf("source path %s does not exist", source)
	}

	delete(srcParentNode.childrenNodes, srcName)
	srcNode.Path = target
	tgtParentNode.childrenNodes[tgtName] = srcNode

	return nil
}

// List returns a list of PathInfo for all nodes under the specified path.
func (nm *NamespaceManager) List(p common.Path) ([]common.PathInfo, error) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	var dir *NsTree
	var parents []*NsTree
	if p == common.Path("/") {
		_, dir = []*NsTree{nm.root}, nm.root
		dir.RLock()
		defer dir.RUnlock()
	} else {
		var err error
		parents, dir, err = nm.lockParents(p, false)
		defer nm.unlockParents(parents, false)
		if err != nil {
			return nil, err
		}
	}

	info := []common.PathInfo{}
	queue := utils.Deque[*NsTree]{}
	queue.PushBack(dir)
	for queue.Length() > 0 {
		current := queue.PopFront()
		for name, child := range current.childrenNodes {
			if child.IsDir && len(child.childrenNodes) != 0 {
				queue.PushBack(child)
			}
			info = append(info, common.PathInfo{
				Path:   string(child.Path),
				IsDir:  child.IsDir,
				Length: child.Length,
				Chunk:  child.Chunks,
				Name:   name,
			})
		}
	}

	return info, nil
}

// RetrievePartitionFromPath splits a path into its directory path and filename or directory name.
func (nm *NamespaceManager) RetrievePartitionFromPath(p common.Path) (string, string) {
	return filepath.Dir(string(p)), filepath.Base(string(p))
}
