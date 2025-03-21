package signatureconfig

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/openshift/oc-mirror/v2/internal/pkg/parser"
	"github.com/stretchr/testify/assert"
)

func TestAddMandatoryRegistries(t *testing.T) {
	testFolder := t.TempDir()
	expectedCacheRegistryFile := filepath.Join(testFolder, "localhost:55000.yaml")
	expectedDestRegistryFile := filepath.Join(testFolder, "mymirror.com.yaml")

	assert.NoError(t, addMandatoryRegistries(testFolder, "localhost:55000", "mymirror.com"))

	assert.FileExists(t, expectedCacheRegistryFile)
	cfg, err := parser.ParseYamlFile[registryConfiguration](expectedCacheRegistryFile)
	assert.NoError(t, err)
	assert.Contains(t, cfg.Docker, "localhost:55000")
	assert.True(t, cfg.Docker["localhost:55000"].UseSigstoreAttachments)

	assert.FileExists(t, expectedDestRegistryFile)
	cfg, err = parser.ParseYamlFile[registryConfiguration](expectedDestRegistryFile)
	assert.NoError(t, err)
	assert.Contains(t, cfg.Docker, "mymirror.com")
	assert.True(t, cfg.Docker["mymirror.com"].UseSigstoreAttachments)

}

func TestRegistry(t *testing.T) {
	testFolder := t.TempDir()
	assert.NoError(t, addRegistry(testFolder, "docker://localhost:55000"))
	assert.FileExists(t, filepath.Join(testFolder, "localhost:55000.yaml"))

	assert.NoError(t, addRegistry(testFolder, "file:///tmp/my-archive"))
	folder := os.DirFS(testFolder)
	yamlFiles, err := fs.Glob(folder, "*.yaml")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(yamlFiles))

	assert.NoError(t, addRegistry(testFolder, "localhost:55000"))
	assert.FileExists(t, filepath.Join(testFolder, "localhost:55000.yaml"))

	assert.NoError(t, addRegistry(testFolder, "oci:///tmp/test"))
	yamlFiles, err = fs.Glob(folder, "*.yaml")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(yamlFiles))

	assert.NoError(t, addRegistry(testFolder, "docker://myregistry"))
	assert.FileExists(t, filepath.Join(testFolder, "myregistry.yaml"))

}
