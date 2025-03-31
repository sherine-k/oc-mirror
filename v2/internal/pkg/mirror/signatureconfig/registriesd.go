package signatureconfig

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strings"

	"github.com/containers/image/v5/types"
	"github.com/containers/storage/pkg/fileutils"
	"github.com/otiai10/copy"
	"gopkg.in/yaml.v2"

	"github.com/openshift/oc-mirror/v2/internal/pkg/consts"
)

var systemRegistriesDirPath = builtinRegistriesDirPath

// builtinRegistriesDirPath is the path to registries.d.
// DO NOT change this, instead see systemRegistriesDirPath above.
const (
	etcDir                   = "/etc"
	builtinRegistriesDirPath = etcDir + "/containers/registries.d"
	containersSubPath        = "containers"
	registriesDSubPath       = "registries.d"
)

// userRegistriesDirPath is the path to the per user registries.d.
var userRegistriesDir = filepath.FromSlash(".config/containers/registries.d")

// defaultUserDockerDir is the default lookaside directory for unprivileged user
var defaultUserDockerDir = filepath.FromSlash(".local/share/containers/sigstore")

// defaultDockerDir is the default lookaside directory for root
var defaultDockerDir = "/var/lib/containers/sigstore"

func SetRegistryConfiguration(sys *types.SystemContext, workingDir, cacheRegistryURL, destinationRegistryURL string) error {
	if sys == nil {
		return fmt.Errorf("systemContext should not be nil")
	}

	// get $HOME
	usr, err := user.Current()
	if err != nil {
		return fmt.Errorf("unable to determine the current user : %w", err)
	}

	// locate the current registries.d that applies
	registriesDir := registriesDirPathWithHomeDir(sys, usr.HomeDir)

	// copy that registries.d content to working-dir/containers/
	customizableRegistriesDir := filepath.Join(workingDir, containersSubPath, registriesDSubPath)
	if err := copyUnderDir(registriesDir, customizableRegistriesDir); err != nil {
		return fmt.Errorf("unable to initialize registries.d configuration for oc-mirror: %w", err)
	}

	sys.RegistriesDirPath = customizableRegistriesDir
	// add a configuration for the cache and destination registries at minimum
	return addMandatoryRegistries(customizableRegistriesDir, cacheRegistryURL, destinationRegistryURL)
}

func registriesDirPathWithHomeDir(sys *types.SystemContext, homeDir string) string {
	// we normally  should look to see if sys.RegistriesDirPath is defined
	// but since oc-mirror doesn´t provide a flag to do that, skipping
	// TODO: have a discussion about introducing such a flag, as in skopeo
	// https://github.com/containers/skopeo/blob/603d37c588b9b8b2a8d82db6dc0136a852a6256d/cmd/skopeo/main.go#L84
	userRegistriesDirPath := filepath.Join(homeDir, userRegistriesDir)
	if err := fileutils.Exists(userRegistriesDirPath); err == nil {
		return userRegistriesDirPath
	}
	if sys != nil && sys.RootForImplicitAbsolutePaths != "" {
		return filepath.Join(sys.RootForImplicitAbsolutePaths, systemRegistriesDirPath)
	}

	return systemRegistriesDirPath
}

func copyUnderDir(folderCopied, destination string) error {
	// TODO should we define copyOptions such as:
	// AddPermission
	// OnDirExists
	// PreserveOwner
	if err := os.MkdirAll(filepath.Dir(destination), 0755); err != nil {
		return err
	}

	if err := copy.Copy(folderCopied, destination); err != nil {
		return err
	}

	return nil
}

func addMandatoryRegistries(customizableRegistriesDir, cacheRegistryURL, destinationRegistryURL string) error {
	if err := addRegistry(customizableRegistriesDir, cacheRegistryURL); err != nil {
		return err
	}
	if err := addRegistry(customizableRegistriesDir, destinationRegistryURL); err != nil {
		return err
	}
	return nil
}

func toFileName(registryURL string) string {
	return registryURL + ".yaml"
}

func addRegistry(customizableRegistriesDir, registryURL string) error {
	if strings.HasPrefix(registryURL, consts.FileProtocol) || strings.HasPrefix(registryURL, consts.DirProtocol) || strings.HasPrefix(registryURL, consts.OciProtocol) {
		// no need to add a configuration file
		// this is probably in mirrorToDisk where the destinationURL is a disk location
		return nil
	}

	registryURL, _ = strings.CutPrefix(registryURL, consts.DockerProtocol)

	// TODO: if file exists, and use-sigstore-attachements isn't configured, what do you do?
	// override? append? exist in error?
	cacheRegistryFileName := toFileName(registryURL)
	// check the cache file exists
	cacheConfigPath := filepath.Join(customizableRegistriesDir, cacheRegistryFileName)

	if _, err := os.Stat(cacheConfigPath); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(filepath.Dir(cacheConfigPath), 0755)
		if err != nil {
			return err
		}
		cacheConfigFile, err := os.Create(cacheConfigPath)
		if err != nil {
			return err
		}
		defer cacheConfigFile.Close()
		// add the cache file yaml
		cacheConfig := registryConfiguration{
			Docker: map[string]registryNamespace{
				registryURL: {
					UseSigstoreAttachments: true,
				},
			},
		}

		ccBytes, err := yaml.Marshal(cacheConfig)
		if err != nil {
			return err
		}
		_, err = cacheConfigFile.Write(ccBytes)
		return err
	} else if err != nil {
		return err
	}
	// if it exists, do you rewrite it? do you leave it?
	return nil
}
