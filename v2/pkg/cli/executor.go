package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"k8s.io/kubectl/pkg/util/templates"

	"github.com/distribution/distribution/v3/configuration"
	dcontext "github.com/distribution/distribution/v3/context"
	"github.com/distribution/distribution/v3/registry"
	_ "github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	distversion "github.com/distribution/distribution/v3/version"
	"github.com/sirupsen/logrus"

	"github.com/google/uuid"

	"github.com/openshift/oc-mirror/v2/pkg/additional"
	"github.com/openshift/oc-mirror/v2/pkg/api/v1alpha2"
	"github.com/openshift/oc-mirror/v2/pkg/api/v1alpha3"
	"github.com/openshift/oc-mirror/v2/pkg/archive"
	"github.com/openshift/oc-mirror/v2/pkg/batch"
	"github.com/openshift/oc-mirror/v2/pkg/clusterresources"
	"github.com/openshift/oc-mirror/v2/pkg/config"
	"github.com/openshift/oc-mirror/v2/pkg/imagebuilder"
	clog "github.com/openshift/oc-mirror/v2/pkg/log"
	"github.com/openshift/oc-mirror/v2/pkg/manifest"
	"github.com/openshift/oc-mirror/v2/pkg/mirror"
	"github.com/openshift/oc-mirror/v2/pkg/operator"
	"github.com/openshift/oc-mirror/v2/pkg/release"
	"github.com/spf13/cobra"
)

const (
	dockerProtocol          string = "docker://"
	ociProtocol             string = "oci://"
	dirProtocol             string = "dir://"
	fileProtocol            string = "file://"
	releaseImageDir         string = "release-images"
	logsDir                 string = "logs"
	workingDir              string = "working-dir"
	additionalImages        string = "additional-images"
	releaseImageExtractDir  string = "hold-release"
	operatorImageExtractDir string = "hold-operator"
	signaturesDir           string = "signatures"
	registryLogFilename     string = "logs/registry.log"
)

var (
	mirrorlongDesc = templates.LongDesc(
		` 
		Create and publish user-configured mirrors with a declarative configuration input.
		used for authenticating to the registries. 

		The podman location for credentials is also supported as a secondary location.

		1. Destination prefix is docker:// - The current working directory will be used.
		2. Destination prefix is oci:// - The destination directory specified will be used.

		`,
	)
	mirrorExamples = templates.Examples(
		`
		# Mirror to a directory
		oc-mirror oci:mirror --config mirror-config.yaml
		`,
	)
	registryLogFile *os.File
)

type ExecutorSchema struct {
	Log              clog.PluggableLoggerInterface
	Config           v1alpha2.ImageSetConfiguration
	Opts             mirror.CopyOptions
	Operator         operator.CollectorInterface
	Release          release.CollectorInterface
	AdditionalImages additional.CollectorInterface
	Mirror           mirror.MirrorInterface
	Manifest         manifest.ManifestInterface
	Batch            batch.BatchInterface
	LocalStorage     registry.Registry
	LocalStorageFQDN string
	ClusterResources clusterresources.GeneratorInterface
	ImageBuilder     imagebuilder.ImageBuilderInterface
}

// NewMirrorCmd - cobra entry point
func NewMirrorCmd(log clog.PluggableLoggerInterface) *cobra.Command {

	global := &mirror.GlobalOptions{
		TlsVerify:    false,
		SecurePolicy: false,
	}

	flagSharedOpts, sharedOpts := mirror.SharedImageFlags()
	flagDepTLS, deprecatedTLSVerifyOpt := mirror.DeprecatedTLSVerifyFlags()
	flagSrcOpts, srcOpts := mirror.ImageSrcFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "src-", "screds")
	flagDestOpts, destOpts := mirror.ImageDestFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "dest-", "dcreds")
	flagRetryOpts, retryOpts := mirror.RetryFlags()

	opts := mirror.CopyOptions{
		Global:              global,
		DeprecatedTLSVerify: deprecatedTLSVerifyOpt,
		SrcImage:            srcOpts,
		DestImage:           destOpts,
		RetryOpts:           retryOpts,
		Dev:                 false,
	}

	ex := &ExecutorSchema{
		Log:  log,
		Opts: opts,
	}

	cmd := &cobra.Command{
		Use:           fmt.Sprintf("%v <destination type>:<destination location>", filepath.Base(os.Args[0])),
		Version:       "v2.0.0-dev-01",
		Short:         "Manage mirrors per user configuration",
		Long:          mirrorlongDesc,
		Example:       mirrorExamples,
		Args:          cobra.MinimumNArgs(1),
		SilenceErrors: false,
		SilenceUsage:  false,
		Run: func(cmd *cobra.Command, args []string) {
			err := ex.Validate(args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
			ex.Complete(args)
			// prepare internal storage
			err = ex.PrepareStorageAndLogs()
			if err != nil {
				log.Error(" %v ", err)
				os.Exit(1)
			}

			err = ex.Run(cmd, args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
		},
	}
	cmd.AddCommand(NewPrepareCommand(log))
	cmd.PersistentFlags().StringVarP(&opts.Global.ConfigPath, "config", "c", "", "Path to imageset configuration file")
	cmd.Flags().StringVar(&opts.Global.LogLevel, "loglevel", "info", "Log level one of (info, debug, trace, error)")
	cmd.Flags().StringVar(&opts.Global.Dir, "dir", "working-dir", "Assets directory")
	cmd.Flags().StringVar(&opts.Global.From, "from", "", "local storage directory for disk to mirror workflow")
	cmd.Flags().Uint16VarP(&opts.Global.Port, "port", "p", 5000, "HTTP port used by oc-mirror's local storage instance")
	cmd.Flags().BoolVarP(&opts.Global.Quiet, "quiet", "q", false, "enable detailed logging when copying images")
	cmd.Flags().BoolVarP(&opts.Global.Force, "force", "f", false, "force the copy and mirror functionality")
	cmd.Flags().BoolVar(&opts.Global.V2, "v2", opts.Global.V2, "Redirect the flow to oc-mirror v2 - PLEASE DO NOT USE that. V2 is still under development and it is not ready to be used.")
	cmd.Flags().BoolVar(&opts.Global.SecurePolicy, "secure-policy", opts.Global.SecurePolicy, "If set (default is false), will enable signature verification (secure policy for signature verification).")
	// nolint: errcheck
	cmd.Flags().MarkHidden("v2")
	cmd.Flags().AddFlagSet(&flagSharedOpts)
	cmd.Flags().AddFlagSet(&flagRetryOpts)
	cmd.Flags().AddFlagSet(&flagDepTLS)
	cmd.Flags().AddFlagSet(&flagSrcOpts)
	cmd.Flags().AddFlagSet(&flagDestOpts)
	return cmd
}

// Validate - cobra validation
func (o ExecutorSchema) Validate(dest []string) error {
	if len(o.Opts.Global.ConfigPath) == 0 {
		return fmt.Errorf("use the --config flag it is mandatory")
	}
	if strings.Contains(dest[0], dockerProtocol) && o.Opts.Global.From == "" {
		return fmt.Errorf("when destination is docker://, diskToMirror workflow is assumed, and the --from argument become mandatory")
	}
	if len(o.Opts.Global.From) > 0 && !strings.Contains(o.Opts.Global.From, fileProtocol) {
		return fmt.Errorf("when --from is used, it must have file:// prefix")
	}
	if strings.Contains(dest[0], fileProtocol) || strings.Contains(dest[0], dockerProtocol) {
		return nil
	} else {
		return fmt.Errorf("destination must have either file:// (mirror to disk) or docker:// (diskToMirror) protocol prefixes")
	}
}

func (o *ExecutorSchema) PrepareStorageAndLogs() error {

	// clean up logs directory
	os.RemoveAll(logsDir)

	// create logs directory
	err := os.MkdirAll(logsDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	//create config file for local registry
	configYamlV0_1 := `
version: 0.1
log:
  accesslog:
    disabled: $$PLACEHOLDER_ACCESS_LOG_OFF$$
  level: $$PLACEHOLDER_LOG_LEVEL$$
  formatter: text
  fields:
    service: registry
storage:
  cache:
    blobdescriptor: inmemory
  filesystem:
    rootdirectory: $$PLACEHOLDER_ROOT$$
http:
  addr: :$$PLACEHOLDER_PORT$$
  headers:
    X-Content-Type-Options: [nosniff]
      #auth:
      #htpasswd:
      #realm: basic-realm
      #path: /etc/registry
health:
  storagedriver:
    enabled: true
    interval: 10s
    threshold: 3
`

	rootDir := ""

	if o.Opts.IsMirrorToDisk() {
		rootDir = strings.TrimPrefix(o.Opts.Destination, fileProtocol)
	} else {
		rootDir = strings.TrimPrefix(o.Opts.Global.From, fileProtocol)
	}

	if rootDir == "" {
		// something went wrong
		return fmt.Errorf("error determining the local storage folder to use")
	}
	configYamlV0_1 = strings.Replace(configYamlV0_1, "$$PLACEHOLDER_ROOT$$", rootDir, 1)
	configYamlV0_1 = strings.Replace(configYamlV0_1, "$$PLACEHOLDER_PORT$$", strconv.Itoa(int(o.Opts.Global.Port)), 1)
	configYamlV0_1 = strings.Replace(configYamlV0_1, "$$PLACEHOLDER_LOG_LEVEL$$", o.Opts.Global.LogLevel, 1)
	if o.Opts.Global.LogLevel == "debug" {
		configYamlV0_1 = strings.Replace(configYamlV0_1, "$$PLACEHOLDER_ACCESS_LOG_OFF$$", "false", 1)
	} else {
		configYamlV0_1 = strings.Replace(configYamlV0_1, "$$PLACEHOLDER_ACCESS_LOG_OFF$$", "true", 1)
	}

	config, err := configuration.Parse(bytes.NewReader([]byte(configYamlV0_1)))

	if err != nil {
		return fmt.Errorf("error parsing local storage configuration : %v\n %s", err, configYamlV0_1)
	}

	regLogger := logrus.New()
	// prepare the logger
	registryLogFile, err = os.Create(registryLogFilename)
	if err != nil {
		regLogger.Warn("Failed to create log file for local storage registry, using default stderr")
	} else {
		regLogger.Out = registryLogFile
	}
	absPath, err := filepath.Abs(registryLogFilename)
	o.Log.Info("local storage registry will log to %s", absPath)
	if err != nil {
		o.Log.Error(err.Error())
	}
	regLogEntry := logrus.NewEntry(regLogger)

	// setup the context
	dcontext.SetDefaultLogger(regLogEntry)
	ctx := dcontext.WithVersion(dcontext.Background(), distversion.Version)
	ctx = dcontext.WithLogger(ctx, regLogEntry)

	reg, err := registry.NewRegistry(ctx, config)
	if err != nil {
		return err
	}
	o.LocalStorage = *reg
	errchan := make(chan error)

	o.Log.Info("starting local storage on %v", config.HTTP.Addr)

	go startLocalRegistry(reg, errchan)
	go panicOnRegistryError(errchan)
	return nil
}

func startLocalRegistry(reg *registry.Registry, errchan chan error) {
	err := reg.ListenAndServe()
	errchan <- err
}

func panicOnRegistryError(errchan chan error) {
	err := <-errchan
	if err != nil {
		panic(err)
	}
}

// Complete - do the final setup of modules
func (o *ExecutorSchema) Complete(args []string) {
	// override log level
	o.Log.Level(o.Opts.Global.LogLevel)
	o.Log.Debug("imagesetconfig file %s ", o.Opts.Global.ConfigPath)
	// read the ImageSetConfiguration
	cfg, err := config.ReadConfig(o.Opts.Global.ConfigPath)
	if err != nil {
		o.Log.Error("imagesetconfig %v ", err)
	}
	o.Log.Trace("imagesetconfig : %v ", cfg)

	// update all dependant modules
	mc := mirror.NewMirrorCopy()
	md := mirror.NewMirrorDelete()
	o.Manifest = manifest.New(o.Log)
	o.Mirror = mirror.New(mc, md)
	o.Config = cfg
	o.Batch = batch.New(o.Log, o.Mirror, o.Manifest)

	// logic to check mode
	var dest string
	if strings.Contains(args[0], fileProtocol) {
		o.Opts.Mode = mirror.MirrorToDisk
		dest = filepath.Join(strings.Split(args[0], "://")[1], workingDir)
		o.Log.Debug("destination %s ", dest)
	} else if strings.Contains(args[0], dockerProtocol) {
		dest = filepath.Join(strings.Split(o.Opts.Global.From, "://")[1], workingDir)
		o.Opts.Mode = mirror.DiskToMirror
	} else {
		o.Log.Error("unable to determine the mode (the destination must be either file:// or docker://)")
	}
	o.Opts.Destination = args[0]
	o.Opts.Global.Dir = dest
	o.Log.Info("mode %s ", o.Opts.Mode)
	o.LocalStorageFQDN = "localhost:" + strconv.Itoa(int(o.Opts.Global.Port))

	client, _ := release.NewOCPClient(uuid.New())

	o.ImageBuilder = imagebuilder.NewBuilder(o.Log, o.Opts)

	signature := release.NewSignatureClient(o.Log, o.Config, o.Opts)
	cn := release.NewCincinnati(o.Log, &o.Config, o.Opts, client, false, signature)
	o.Release = release.New(o.Log, o.Config, o.Opts, o.Mirror, o.Manifest, cn, o.LocalStorageFQDN, o.ImageBuilder)
	o.Operator = operator.New(o.Log, o.Config, o.Opts, o.Mirror, o.Manifest, o.LocalStorageFQDN)
	o.AdditionalImages = additional.New(o.Log, o.Config, o.Opts, o.Mirror, o.Manifest, o.LocalStorageFQDN)
	o.ClusterResources = clusterresources.New(o.Log, o.Config, o.Opts)
}

// Run - start the mirror functionality
func (o *ExecutorSchema) Run(cmd *cobra.Command, args []string) error {
	startTime := time.Now()
	// clean up logs directory
	os.RemoveAll(logsDir)

	// create logs directory
	err := os.MkdirAll(logsDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// make sure we always get multi-arch images
	o.Opts.MultiArch = "all"

	if o.Opts.IsMirrorToDisk() {

		// ensure working dir exists
		err := os.MkdirAll(workingDir, 0755)
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}

		// create signatures directory
		o.Log.Trace("creating signatures directory %s ", o.Opts.Global.Dir+"/"+signaturesDir)
		err = os.MkdirAll(o.Opts.Global.Dir+"/"+signaturesDir, 0755)
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}

		// create release-images directory
		o.Log.Trace("creating release images directory %s ", o.Opts.Global.Dir+"/"+releaseImageDir)
		err = os.MkdirAll(o.Opts.Global.Dir+"/"+releaseImageDir, 0755)
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}

		// create release cache dir
		o.Log.Trace("creating release cache directory %s ", o.Opts.Global.Dir+"/"+releaseImageExtractDir)
		err = os.MkdirAll(o.Opts.Global.Dir+"/"+releaseImageExtractDir, 0755)
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}

		// create operator cache dir
		o.Log.Trace("creating operator cache directory %s ", o.Opts.Global.Dir+"/"+operatorImageExtractDir)
		err = os.MkdirAll(o.Opts.Global.Dir+"/"+operatorImageExtractDir, 0755)
		if err != nil {
			o.Log.Error(" %v ", err)
			return err
		}
	}

	var allRelatedImages []v1alpha3.CopyImageSchema

	// do releases
	imgs, err := o.Release.ReleaseImageCollector(cmd.Context())
	if err != nil {
		cleanUp()
		return err
	}
	o.Log.Info("total release images to copy %d ", len(imgs))
	o.Opts.ImageType = "release"
	allRelatedImages = mergeImages(allRelatedImages, imgs)

	// do operators
	imgs, err = o.Operator.OperatorImageCollector(cmd.Context())
	if err != nil {
		cleanUp()
		return err
	}
	o.Log.Info("total operator images to copy %d ", len(imgs))
	o.Opts.ImageType = "operator"
	allRelatedImages = mergeImages(allRelatedImages, imgs)

	// do additionalImages
	imgs, err = o.AdditionalImages.AdditionalImagesCollector(cmd.Context())
	if err != nil {
		cleanUp()
		return err
	}
	o.Log.Info("total additional images to copy %d ", len(imgs))
	allRelatedImages = mergeImages(allRelatedImages, imgs)

	collectionFinish := time.Now()

	ctx := cmd.Context()

	//call the batch worker
	err = o.Batch.Worker(ctx, allRelatedImages, o.Opts)
	if err != nil {
		cleanUp()
		return err
	}

	// Prepare tar.gz when mirror to disk
	if o.Opts.IsMirrorToDisk() {
		// TODO First stop the registry

		// Next, generate the archive
		archiver, err := archive.NewMirrorArchive(ctx, &o.Opts, o.Opts.Global.Dir, o.Opts.Global.ConfigPath, o.Opts.Global.Dir, o.Opts.Global.Dir)
		if err != nil {
			cleanUp()
			return err
		}
		defer archiver.Close()
		archiveFile, err := archiver.BuildArchive(allRelatedImages)
		if err != nil {
			cleanUp()
			return err
		}
		o.Log.Info("archive file generated: %v ", archiveFile)

	}

	//create IDMS/ITMS
	if o.Opts.IsDiskToMirror() {
		err = o.ClusterResources.IDMSGenerator(cmd.Context(), allRelatedImages, o.Opts)
		if err != nil {
			cleanUp()
			return err
		}
	}
	mirrorFinish := time.Now()
	o.Log.Info("start time: %v\ncollection time: %v\nmirror time: %v", startTime, collectionFinish, mirrorFinish)
	if err != nil {
		cleanUp()
		return err
	}

	defer cleanUp()
	return nil
}

// mergeImages - simple function to append related images
// nolint
func mergeImages(base, in []v1alpha3.CopyImageSchema) []v1alpha3.CopyImageSchema {
	base = append(base, in...)
	return base
}

// cleanUp - utility to clean directories
func cleanUp() {
	// close registry log file
	err := registryLogFile.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error closing log file %s: %v\n", registryLogFilename, err)
	}
	// clean up logs directory
	os.RemoveAll(logsDir)
}

func NewPrepareCommand(log clog.PluggableLoggerInterface) *cobra.Command {
	global := &mirror.GlobalOptions{
		TlsVerify:    false,
		SecurePolicy: false,
	}

	flagSharedOpts, sharedOpts := mirror.SharedImageFlags()
	flagDepTLS, deprecatedTLSVerifyOpt := mirror.DeprecatedTLSVerifyFlags()
	flagSrcOpts, srcOpts := mirror.ImageSrcFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "src-", "screds")
	flagDestOpts, destOpts := mirror.ImageDestFlags(global, sharedOpts, deprecatedTLSVerifyOpt, "dest-", "dcreds")
	flagRetryOpts, retryOpts := mirror.RetryFlags()

	opts := mirror.CopyOptions{
		Global:              global,
		DeprecatedTLSVerify: deprecatedTLSVerifyOpt,
		SrcImage:            srcOpts,
		DestImage:           destOpts,
		RetryOpts:           retryOpts,
		Dev:                 false,
	}

	ex := &ExecutorSchema{
		Log:  log,
		Opts: opts,
	}
	cmd := &cobra.Command{
		Use:   "prepare",
		Short: "Queries Cincinnati for the required releases to mirror, and verifies their existence in the local cache",
		Run: func(cmd *cobra.Command, args []string) {
			err := ex.ValidatePrepare(args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
			ex.CompletePrepare(args)
			// prepare internal storage
			err = ex.PrepareStorageAndLogs()
			if err != nil {
				log.Error(" %v ", err)
				os.Exit(1)
			}

			err = ex.RunPrepare(cmd, args)
			if err != nil {
				log.Error("%v ", err)
				os.Exit(1)
			}
		},
	}
	cmd.PersistentFlags().StringVarP(&opts.Global.ConfigPath, "config", "c", "", "Path to imageset configuration file")
	cmd.Flags().StringVar(&opts.Global.LogLevel, "loglevel", "info", "Log level one of (info, debug, trace, error)")
	cmd.Flags().StringVar(&opts.Global.Dir, "dir", "working-dir", "Assets directory")
	cmd.Flags().StringVar(&opts.Global.From, "from", "", "local storage directory for disk to mirror workflow")
	cmd.Flags().Uint16VarP(&opts.Global.Port, "port", "p", 5000, "HTTP port used by oc-mirror's local storage instance")
	cmd.Flags().BoolVar(&opts.Global.V2, "v2", opts.Global.V2, "Redirect the flow to oc-mirror v2 - PLEASE DO NOT USE that. V2 is still under development and it is not ready to be used.")
	// nolint: errcheck
	cmd.Flags().MarkHidden("v2")
	cmd.Flags().AddFlagSet(&flagSharedOpts)
	cmd.Flags().AddFlagSet(&flagRetryOpts)
	cmd.Flags().AddFlagSet(&flagDepTLS)
	cmd.Flags().AddFlagSet(&flagSrcOpts)
	cmd.Flags().AddFlagSet(&flagDestOpts)
	return cmd
}

// Validate - cobra validation
func (o ExecutorSchema) ValidatePrepare(dest []string) error {
	if len(o.Opts.Global.ConfigPath) == 0 {
		return fmt.Errorf("use the --config flag it is mandatory")
	}
	if o.Opts.Global.From == "" {
		return fmt.Errorf("with prepare command, the --from argument become mandatory(prefix : file://)")
	}
	if !strings.Contains(o.Opts.Global.From, fileProtocol) {
		return fmt.Errorf("when --from is used, it must have file:// prefix")
	}
	return nil
}

func (o *ExecutorSchema) CompletePrepare(args []string) {
	// override log level
	o.Log.Level(o.Opts.Global.LogLevel)
	o.Log.Debug("imagesetconfig file %s ", o.Opts.Global.ConfigPath)
	// read the ImageSetConfiguration
	cfg, err := config.ReadConfig(o.Opts.Global.ConfigPath)
	if err != nil {
		o.Log.Error("imagesetconfig %v ", err)
	}
	o.Log.Trace("imagesetconfig : %v ", cfg)

	// update all dependant modules
	mc := mirror.NewMirrorCopy()
	o.Manifest = manifest.New(o.Log)
	o.Mirror = mirror.New(mc, nil)
	o.Config = cfg

	dest := filepath.Join(strings.Split(o.Opts.Global.From, "://")[1], workingDir)

	o.Opts.Global.Dir = dest

	o.LocalStorageFQDN = "localhost:" + strconv.Itoa(int(o.Opts.Global.Port))
	o.Opts.Mode = mirror.Prepare
	o.Log.Info("mode %s ", o.Opts.Mode)
	client, _ := release.NewOCPClient(uuid.New())

	signature := release.NewSignatureClient(o.Log, o.Config, o.Opts)
	cn := release.NewCincinnati(o.Log, &o.Config, o.Opts, client, false, signature)
	o.Release = release.New(o.Log, o.Config, o.Opts, o.Mirror, o.Manifest, cn, o.LocalStorageFQDN, o.ImageBuilder)
	o.Operator = operator.New(o.Log, o.Config, o.Opts, o.Mirror, o.Manifest, o.LocalStorageFQDN)
	o.AdditionalImages = additional.New(o.Log, o.Config, o.Opts, o.Mirror, o.Manifest, o.LocalStorageFQDN)

}

func (o *ExecutorSchema) RunPrepare(cmd *cobra.Command, args []string) error {

	// clean up logs directory
	os.RemoveAll(logsDir)

	// create logs directory
	err := os.MkdirAll(logsDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}
	err = os.MkdirAll(workingDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// create signatures directory
	o.Log.Trace("creating signatures directory %s ", o.Opts.Global.Dir+"/"+signaturesDir)
	err = os.MkdirAll(o.Opts.Global.Dir+"/"+signaturesDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// create release-images directory
	o.Log.Trace("creating release images directory %s ", o.Opts.Global.Dir+"/"+releaseImageDir)
	err = os.MkdirAll(o.Opts.Global.Dir+"/"+releaseImageDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// create release cache dir
	o.Log.Trace("creating release cache directory %s ", o.Opts.Global.Dir+"/"+releaseImageExtractDir)
	err = os.MkdirAll(o.Opts.Global.Dir+"/"+releaseImageExtractDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// create operator cache dir
	o.Log.Trace("creating operator cache directory %s ", o.Opts.Global.Dir+"/"+operatorImageExtractDir)
	err = os.MkdirAll(o.Opts.Global.Dir+"/"+operatorImageExtractDir, 0755)
	if err != nil {
		o.Log.Error(" %v ", err)
		return err
	}

	// creating file for storing list of chached images
	cachedImagesFilePath := filepath.Join(logsDir, "cached-images.txt")
	cachedImagesFile, err := os.Create(cachedImagesFilePath)
	if err != nil {
		return err
	}
	defer cachedImagesFile.Close()

	var allRelatedImages []v1alpha3.CopyImageSchema

	// do releases
	imgs, err := o.Release.ReleaseImageCollector(cmd.Context())
	if err != nil {
		cleanUp()
		return err
	}
	o.Log.Info("total release images to copy %d ", len(imgs))
	o.Opts.ImageType = "release"
	allRelatedImages = mergeImages(allRelatedImages, imgs)
	imagesAvailable := map[string]bool{}
	atLeastOneMissing := false
	var buff bytes.Buffer
	for _, img := range allRelatedImages {
		buff.WriteString(img.Destination + "\n")
		exists, err := o.Mirror.Check(cmd.Context(), img.Destination, &o.Opts)
		if err != nil {
			o.Log.Warn("unable to check existence of %s in local cache: %v", img.Destination, err)
		}
		if err != nil || !exists {
			atLeastOneMissing = true
		}
		imagesAvailable[img.Destination] = exists

	}

	_, err = cachedImagesFile.Write(buff.Bytes())
	if err != nil {
		return err
	}
	if atLeastOneMissing {
		o.Log.Error("missing images: ")
		for img, exists := range imagesAvailable {
			if !exists {
				o.Log.Error("%s", img)
			}
		}
		return fmt.Errorf("all images necessary for mirroring are not available in the cache. \nplease re-run the mirror to disk process")
	}

	o.Log.Info("all %d images required for mirroring are available in local cache. You may proceed with mirroring from disk to disconnected registry", len(imagesAvailable))
	o.Log.Info("full list in : %s", cachedImagesFilePath)
	return nil
}
