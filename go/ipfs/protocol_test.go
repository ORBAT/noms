package ipfs

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/attic-labs/noms/go/spec"
	_ "github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const repoTempPrefix = "noms-ipfs-test-"

func mustRegister() func() {
	if err := RegisterProtocols(); err != nil {
		panic(err)
	}

	return func() {
		UnregisterProtocols()
	}
}

func mustTempDir(prefix string) (name string) {
	name, err := ioutil.TempDir("", prefix)
	if err != nil {
		panic("couldn't create temporary directory: " + err.Error())
	}
	return name
}

func TestProtocol(t *testing.T) {
	defer mustRegister()()
	for _, proto := range []string{"ipfs", "ipfs-local"} {
		t.Run(proto, func(t *testing.T) {
			tempDir := mustTempDir(repoTempPrefix)
			defer os.RemoveAll(tempDir)
			specStr := spec.CreateDatabaseSpecString(proto, tempDir)
			_, err := spec.ForDatabase(specStr)
			require.NoError(t, err, "creating spec from database string", specStr, "should not fail")
		})
	}
}
