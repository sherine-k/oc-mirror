package cli

const (
	collecAllPrefix                      = "[CollectAll] "
	dockerProtocol                string = "docker://"
	ociProtocol                   string = "oci://"
	dirProtocol                   string = "dir://"
	fileProtocol                  string = "file://"
	releaseImageDir               string = "release-images"
	logsDir                       string = "logs"
	workingDir                    string = "working-dir"
	ocmirrorRelativePath          string = ".oc-mirror"
	cacheRelativePath             string = ".oc-mirror/.cache"
	cacheEnvVar                   string = "OC_MIRROR_CACHE"
	additionalImages              string = "additional-images"
	releaseImageExtractDir        string = "hold-release"
	cincinnatiGraphDataDir        string = "cincinnati-graph-data"
	operatorImageExtractDir       string = "hold-operator"
	signaturesDir                 string = "signatures"
	registryLogFilename           string = "registry.log"
	startMessage                  string = "starting local storage on localhost:%v"
	dryRunOutDir                  string = "dry-run"
	mappingFile                   string = "mapping.txt"
	missingImgsFile               string = "missing.txt"
	clusterResourcesDir           string = "cluster-resources"
	maxParallelLayerDownloads     uint   = 10
	limitOverallParallelDownloads uint   = 200
	graphBaseImage                       = "registry.redhat.io/ubi9/ubi:latest"
)
