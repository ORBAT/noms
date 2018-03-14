package chunks

import (
	"github.com/attic-labs/noms/go/d"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/attic-labs/noms/go/constants"
	"github.com/attic-labs/noms/go/hash"
)

type ChunkStoreTestSuite struct {
	suite.Suite
	Factory Factory
}

func (suite *ChunkStoreTestSuite) TestChunkStorePut() {
	store := suite.Factory.CreateStore("ns")
	input := "abc"
	c := NewChunk([]byte(input))
	store.Put(c)
	h := c.Hash()

	// Reading it via the API should work.
	AssertInputInStore(input, h, store, suite.Assert())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreRoot() {
	store := suite.Factory.CreateStore("ns")
	oldRoot := store.Root()
	suite.True(oldRoot.IsEmpty())

	bogusRoot := hash.Parse("8habda5skfek1265pc5d5l1orptn5dr0")
	newRoot := hash.Parse("8la6qjbh81v85r6q67lqbfrkmpds14lg")

	// Try to update root with bogus oldRoot
	result := store.Commit(newRoot, bogusRoot)
	suite.False(result)

	// Now do a valid root update
	result = store.Commit(newRoot, oldRoot)
	suite.True(result)
}

func (suite *ChunkStoreTestSuite) TestChunkStoreCommitPut() {
	name := "ns"
	store := suite.Factory.CreateStore(name)
	input := "abc"
	c := NewChunk([]byte(input))
	store.Put(c)
	h := c.Hash()

	// Reading it via the API should work...
	AssertInputInStore(input, h, store, suite.Assert())
	// ...but it shouldn't be persisted yet
	AssertInputNotInStore(h, suite.Factory.CreateStore(name), suite.Assert())

	store.Commit(h, store.Root()) // Commit persists Chunks
	AssertInputInStore(input, h, store, suite.Assert())
	AssertInputInStore(input, h, suite.Factory.CreateStore(name), suite.Assert())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreGetNonExisting() {
	store := suite.Factory.CreateStore("ns")
	h := hash.Parse("11111111111111111111111111111111")
	c := store.Get(h)
	suite.True(c.IsEmpty())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreVersion() {
	store := suite.Factory.CreateStore("ns")
	oldRoot := store.Root()
	suite.True(oldRoot.IsEmpty())
	newRoot := hash.Parse("11111222223333344444555556666677")
	suite.True(store.Commit(newRoot, oldRoot))

	suite.Equal(constants.NomsVersion, store.Version())
}

func (suite *ChunkStoreTestSuite) TestChunkStoreCommitUnchangedRoot() {
	store1, store2 := suite.Factory.CreateStore("ns"), suite.Factory.CreateStore("ns")
	input := "abc"
	c := NewChunk([]byte(input))
	store1.Put(c)
	h := c.Hash()

	// Reading c from store1 via the API should work...
	AssertInputInStore(input, h, store1, suite.Assert())
	// ...but not store2.
	AssertInputNotInStore(h, store2, suite.Assert())

	store1.Commit(store1.Root(), store1.Root())
	store2.Rebase()
	// Now, reading c from store2 via the API should work...
	AssertInputInStore(input, h, store2, suite.Assert())
}

func AssertInputInStore(input string, h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(h)
	assert.False(chunk.IsEmpty(), "Shouldn't get empty chunk for %s", h.String())
	assert.Equal(input, string(chunk.Data()))
}

func AssertInputNotInStore(h hash.Hash, s ChunkStore, assert *assert.Assertions) {
	chunk := s.Get(h)
	assert.True(chunk.IsEmpty(), "Shouldn't get non-empty chunk for %s: %v", h.String(), chunk)
}

type TestStorage struct {
	MemoryStorage
}

func (t *TestStorage) NewView() *TestStoreView {
	return &TestStoreView{ChunkStore: t.MemoryStorage.NewView()}
}

type TestStoreView struct {
	ChunkStore
	Reads  int
	Hases  int
	Writes int
}

func (s *TestStoreView) Get(h hash.Hash) Chunk {
	s.Reads++
	return s.ChunkStore.Get(h)
}

func (s *TestStoreView) GetMany(hashes hash.HashSet, foundChunks chan *Chunk) {
	s.Reads += len(hashes)
	s.ChunkStore.GetMany(hashes, foundChunks)
}

func (s *TestStoreView) Has(h hash.Hash) bool {
	s.Hases++
	return s.ChunkStore.Has(h)
}

func (s *TestStoreView) HasMany(hashes hash.HashSet) hash.HashSet {
	s.Hases += len(hashes)
	return s.ChunkStore.HasMany(hashes)
}

func (s *TestStoreView) Put(c Chunk) {
	s.Writes++
	s.ChunkStore.Put(c)
}

type TestStoreFactory struct {
	stores map[string]*TestStorage
}

func NewTestStoreFactory() *TestStoreFactory {
	return &TestStoreFactory{map[string]*TestStorage{}}
}

func (f *TestStoreFactory) CreateStore(ns string) ChunkStore {
	if f.stores == nil {
		d.Panic("Cannot use TestStoreFactory after Shutter().")
	}
	if ts, present := f.stores[ns]; present {
		return ts.NewView()
	}
	f.stores[ns] = &TestStorage{}
	return f.stores[ns].NewView()
}

func (f *TestStoreFactory) Shutter() {
	f.stores = nil
}
